import feaas.objects as objs
from feaas.psee.util import load_registers
from feaas.stream import util
from feaas.util import common
from google.protobuf.json_format import Parse, MessageToDict
import json
import logging
import os
import requests
import time
import traceback
import uuid
import io
from decimal import Decimal
from feaas.actions.psee_validator import PseeScriptValidator

log = logging.getLogger(__name__)
logging.basicConfig(level=os.environ.get("LOGLEVEL", "INFO"))


class PlusScriptExecutionEngine(object):


    def __init__(self, dao, executor, max_iterations=100, max_actions=500):
        self.dao = dao
        if self.dao is not None:
            self.docstore = dao.get_docstore()
            self.blobstore = dao.get_blobstore()
            self.streams = dao.get_streams()
        else:
            self.docstore = None
            self.blobstore = None
            self.streams = None
        self.executor = executor
        self.max_iterations = max_iterations
        self.max_actions = max_actions


    def _create_job(self, hostname, username, pscript: objs.PlusScript, data={}) -> objs.PlusScriptJob:
        if 'job_id' in data:
            job_id = data['job_id']
            del data['job_id']
        else:
            job_id = str(uuid.uuid4())
        owner = f'{hostname}/{username}/job'
        object_id = f'{owner}.{job_id}'
        # TODO: for file trigger, src_key needs to be injected
        registers, status, err_message = load_registers(username, pscript, data)
        
        # Add Receipt handling to registers
        for key, val in registers.items():
            if isinstance(val, objs.Receipt):
                reg = objs.AnyType()
                reg.ptype = objs.ParameterType.RECEIPT
                reg.receipt.CopyFrom(val)
                registers[key] = reg
                registers[f"mainInput:{key}"] = reg

        job = objs.PlusScriptJob(
            object_id=object_id,
            owner=owner,
            username=username,
            status=status,
            label=pscript.label,
            script = pscript,
            started_at = int(time.time()),
            # steps=pscript.steps,
            # outputs=pscript.outputs,
            registers=registers,
            iteration=0,
            action_count=0,
            # ttl=int(time.time()) + 60*5,
            err_message=err_message
        )
        return job


    def _attempt_to_prepare_node_inputs(self, node, registers, src_links):
        # Returns None if not ready to run, otherwise input is prepared
        data = {}
        # TODO: reverse links
        # TODO: get source
        # TODO: pull source from registers
        if node.unique_id not in src_links:
            return data
        srcs_mine = src_links[node.unique_id]
        for in_param in node.inputs:
            if in_param.var_name not in srcs_mine:
                continue
            link = srcs_mine[in_param.var_name]
            src = f"{link.source}:{link.sourceHandle}"
            reg_value = registers[src]
            if reg_value is None:
                print(f"No value {src} requested in {node.unique_id}")
                continue
            else:
                val = util.any_type_resolved(reg_value)
                data[in_param.var_name] = val
        return data


    def start_script(self, hostname, username, pscript: objs.PlusScript, data={}) -> objs.PlusScriptJob:
        job = self._create_job(hostname, username, pscript, data)
        # validator = PseeScriptValidator()
        # receipt = validator.execute_action('ps_obj_id', 'data_obj_id')
        # outputs = receipt.outputs
        
        # if outputs['missing_inputs'].ival > 0:
        #     job.status = objs.PlusScriptStatus.FAILED
        #     job.err_message = 'There is at least one missing input data in the script'
        #     return job
    
        # if outputs['missing_links'].ival > 0:
        #     job.status = objs.PlusScriptStatus.FAILED
        #     job.err_message = 'There is at least one missing link in the script'
        #     return job
        
        o = MessageToDict(job, preserving_proto_field_name=True)
        o = common.protoBufIntFix(o)
        x = self.docstore.save_document(job.object_id, o)
        return job


    def _set_job_to_failed(self, job, msg_sys, msg_user):
        if job.status != objs.PlusScriptStatus.FAILED:
            job.status = objs.PlusScriptStatus.FAILED
            job.err_message = msg_user
            msg = {
                'type': f'psee-ceiling',
                'subject': msg_sys,
                'job_object_id': job.object_id
            }
            self.dao.get_streams().update_feed('sys', int(time.time() * 1000), msg)
        return job


    def run_job(self, job: objs.PlusScriptJob) -> objs.PlusScriptJob:
        job = self._run_job(job)
        d = MessageToDict(job, preserving_proto_field_name=True)
        d = common.clean_json_dict(d)
        _ = self.docstore.update_document(job.object_id, d)
        return job
    
    def run_job_and_save_file_receipt(self, job: objs.PlusScriptJob, dest_prefix) -> objs.PlusScriptJob:
        job = self._run_job(job)
        d = MessageToDict(job, preserving_proto_field_name=True)
        d = common.clean_json_dict(d)
        _ = self.docstore.update_document(job.object_id, d)

        dest_key = f'{dest_prefix}{str(uuid.uuid4())}.json'
        receipt = self.docstore.get_document(job.object_id)
        receipt_str  = json.dumps(receipt, cls=common.DecimalEncoder, sort_keys=True)
        buf = io.BytesIO(receipt_str.encode('utf-8'))
        buf.seek(0)
        file_byte = buf.read()
        self.blobstore.save_blob(dest_key, file_byte)

        return receipt, dest_key
    

    def run_job_and_save_stream_receipt(self, job: objs.PlusScriptJob, stream_id) -> objs.PlusScriptJob:
        job = self._run_job(job)
        d = MessageToDict(job, preserving_proto_field_name=True)
        d = common.clean_json_dict(d)
        _ = self.docstore.update_document(job.object_id, d)

        stream_key = f'{stream_id}{str(uuid.uuid4())}.json'
        receipt = self.docstore.get_document(job.object_id)
        receipt_str  = json.dumps(receipt, cls=common.DecimalEncoder, sort_keys=True)
        buf = io.BytesIO(receipt_str.encode('utf-8'))
        buf.seek(0)
        file_byte = buf.read()
        self.blobstore.save_blob(stream_key, file_byte)

        items = [{'stream_key': stream_key,
                  'timestamp': Decimal(time.time() * 1000)}]

        self.streams.batch_write_stream(items, stream_id)

        return receipt, stream_id


    def _reverse_links(self, links):
        reverse_links = {}
        for link in links:
            if link.target not in reverse_links:
                reverse_links[link.target] = {}
            if link.targetHandle not in reverse_links[link.target]:
                reverse_links[link.target][link.targetHandle] = {}
            reverse_links[link.target][link.targetHandle] = link #[src1, src2]

        return reverse_links


    def _run_job(self, job: objs.PlusScriptJob) -> objs.PlusScriptJob:
        reverse_links = self._reverse_links(job.script.links)
        if job.iteration >= self.max_iterations:
            msg_sys = f'A PlusScript belonging to {job.username} job ran for {self.max_iterations} iterations and then paused.'
            msg_user = f'This job was paused due to hitting the max iterations of {self.max_iterations}.  Please contact support if you wish to request an increase.'
            return self._set_job_to_failed(job, msg_sys, msg_user)
        # First pass: Process ACTION, RECEIPT_META, BUNDLER, and STATIC nodes
        for node in job.script.nodes:
            if node.unique_id in job.receipts:
                pass
            elif node.ntype in [objs.PlusScriptNodeType.UPDATE_VALUES, objs.PlusScriptNodeType.UI_FEEDBACK]:
                # Skip UPDATE_VALUES and UI_FEEDBACK nodes in first pass
                continue
            elif node.ntype == objs.PlusScriptNodeType.ACTION:
                if job.action_count >= self.max_actions:
                    msg_sys =f'A PlusScript belonging to {job.username} job ran for {job.action_count} >= {self.max_actions} max actions and then paused.'
                    msg_user = f'This job was paused due to hitting the max actions of {self.max_actions} in a single script run.  Please contact support if you wish to request an increase.'
                    return self._set_job_to_failed(job, msg_sys, msg_user)
                data = self._attempt_to_prepare_node_inputs(node, job.registers, reverse_links)
                job.status = objs.PlusScriptStatus.RUNNING
                print("NODE", data)
                if data is None:
                    print("-----")
                    print(job.registers.keys())
                    1/0
                if data is not None:
                    # TODO: change this to SQS, except for local ones
                    receipt = self.executor.begin_action_execution(node.action_id, job.username, data)
                    job.receipts[node.unique_id].CopyFrom(receipt)
                    for k in receipt.outputs.keys():
                        reg_key = f'{node.unique_id}:{k}'
                        output_val = receipt.outputs[k]
                        job.registers[reg_key].CopyFrom(output_val)

                    job.action_count += 1

                    if not(receipt.success):
                        job.iteration += 1
                        job.status = objs.PlusScriptStatus.FAILED
                        job.err_message = receipt.error_message
                        return job
                    
            elif node.ntype == objs.PlusScriptNodeType.RECEIPT_META:
                data = self._attempt_to_prepare_node_inputs(node, job.registers, reverse_links)
                if data is None or "receipt" not in data:
                    print(f"Skipping node {node.unique_id} because data is None or 'receipt' not in data")
                    continue

                source_receipt = data["receipt"]
                if not isinstance(source_receipt, objs.Receipt):
                    print(f"Skipping node {node.unique_id} because source_receipt is not an instance of objs.Receipt")
                    continue

                # Extract success and error_message and put them into the Registers
                job.registers[f"{node.unique_id}:success"].ptype = objs.ParameterType.BOOLEAN
                job.registers[f"{node.unique_id}:success"].bval = source_receipt.success

                job.registers[f"{node.unique_id}:error_message"].ptype = objs.ParameterType.STRING
                job.registers[f"{node.unique_id}:error_message"].sval = source_receipt.error_message

                # Extract outputs and save them as a JSON type in the Registers
                flat_outputs = {}
                for key, any_val in source_receipt.outputs.items():
                    py_value = util.any_type_resolved(any_val)
                    flat_outputs[key] = py_value

                job.registers[f"{node.unique_id}:outputs"].ptype = objs.ParameterType.JSON
                job.registers[f"{node.unique_id}:outputs"].sval = json.dumps(flat_outputs)

            elif node.ntype == objs.PlusScriptNodeType.BUNDLER:
                data = self._attempt_to_prepare_node_inputs(node, job.registers, reverse_links)
                if data is None:
                    print(f"Skipping bundle node {node.unique_id} because data is None")
                    continue

                # Bundle nodes merge all their inputs into a JSON object
                bundle_output = {}
                for input_param in node.inputs:
                    var_name = input_param.var_name
                    if var_name in data:
                        bundle_output[var_name] = data[var_name]

                # Save the bundled output as JSON in the registers
                job.registers[f"{node.unique_id}:json"].ptype = objs.ParameterType.JSON
                job.registers[f"{node.unique_id}:json"].sval = json.dumps(bundle_output)
                print(f"Bundle name and json: {node.unique_id} -> {bundle_output}")

        # Second pass: Process UPDATE_VALUES and UI_FEEDBACK nodes after all actions are complete
        for node in job.script.nodes:
            if node.ntype == objs.PlusScriptNodeType.UPDATE_VALUES:
                print(f"[PSEE] Second pass: Processing UPDATE_VALUES node {node.unique_id}")
                data = self._attempt_to_prepare_node_inputs(node, job.registers, reverse_links)
                
                if data is None:
                    data = {}
                
                print(f"[PSEE] UPDATE_VALUES node {node.unique_id} received data: {data}")

                # UPDATE_VALUES nodes collect dashboard parameter updates
                update_values = {}
                for input_param in node.inputs:
                    var_name = input_param.var_name
                    if var_name in data:
                        value = data[var_name]
                        any_type_val = objs.AnyType()
                        
                        # Determine the appropriate type and set the value
                        if isinstance(value, bool):
                            any_type_val.ptype = objs.ParameterType.BOOLEAN
                            any_type_val.bval = value
                        elif isinstance(value, int):
                            any_type_val.ptype = objs.ParameterType.INTEGER
                            any_type_val.ival = value
                        elif isinstance(value, float):
                            any_type_val.ptype = objs.ParameterType.FLOAT
                            any_type_val.dval = value
                        elif isinstance(value, str):
                            any_type_val.ptype = objs.ParameterType.STRING
                            any_type_val.sval = value
                        elif isinstance(value, list):
                            any_type_val.ptype = objs.ParameterType.LIST
                            any_type_val.svals.extend([str(v) for v in value])
                        elif isinstance(value, dict):
                            any_type_val.ptype = objs.ParameterType.JSON
                            any_type_val.sval = json.dumps(value)
                        else:
                            any_type_val.ptype = objs.ParameterType.STRING
                            any_type_val.sval = str(value)
                        
                        update_values[var_name] = any_type_val
                        print(f"[PSEE] UPDATE_VALUES node added update for parameter '{var_name}': {value}")

                # Store the update values in the job registers
                for param_name, any_type_val in update_values.items():
                    register_key = f"{node.unique_id}:update_value:{param_name}"
                    job.registers[register_key].CopyFrom(any_type_val)

                # Mark as processed
                processed_marker = objs.AnyType()
                processed_marker.ptype = objs.ParameterType.BOOLEAN
                processed_marker.bval = True
                job.registers[f"{node.unique_id}:processed"].CopyFrom(processed_marker)

            elif node.ntype == objs.PlusScriptNodeType.UI_FEEDBACK:
                print(f"[PSEE] Second pass: Processing UI_FEEDBACK node {node.unique_id}")
                data = self._attempt_to_prepare_node_inputs(node, job.registers, reverse_links)
                
                if data is None:
                    data = {}
                
                print(f"[PSEE] UI_FEEDBACK node {node.unique_id} received data: {data}")

                # UI_FEEDBACK nodes collect dashboard feedback values
                for input_param in node.inputs:
                    var_name = input_param.var_name
                    if var_name in data:
                        value = data[var_name]
                        any_type_val = objs.AnyType()
                        
                        if isinstance(value, bool):
                            any_type_val.ptype = objs.ParameterType.BOOLEAN
                            any_type_val.bval = value
                        elif isinstance(value, int):
                            any_type_val.ptype = objs.ParameterType.INTEGER
                            any_type_val.ival = value
                        elif isinstance(value, float):
                            any_type_val.ptype = objs.ParameterType.FLOAT
                            any_type_val.dval = value
                        elif isinstance(value, str):
                            any_type_val.ptype = objs.ParameterType.STRING
                            any_type_val.sval = value
                        elif isinstance(value, list):
                            any_type_val.ptype = objs.ParameterType.LIST
                            any_type_val.svals.extend([str(v) for v in value])
                        elif isinstance(value, dict):
                            any_type_val.ptype = objs.ParameterType.JSON
                            any_type_val.sval = json.dumps(value)
                        else:
                            any_type_val.ptype = objs.ParameterType.STRING
                            any_type_val.sval = str(value)
                        
                        # Store the UI feedback value
                        register_key = f"{node.unique_id}:{var_name}"
                        job.registers[register_key].CopyFrom(any_type_val)
                        print(f"[PSEE] UI_FEEDBACK node stored '{var_name}': {value}")

                # Mark as processed
                processed_marker = objs.AnyType()
                processed_marker.ptype = objs.ParameterType.BOOLEAN
                processed_marker.bval = True
                job.registers[f"{node.unique_id}:processed"].CopyFrom(processed_marker)

        completed = True
        for node in job.script.nodes:
            if node.ntype == objs.PlusScriptNodeType.ACTION:
                if node.unique_id not in job.receipts:
                    completed = False
            elif node.ntype == objs.PlusScriptNodeType.BUNDLER:
                # Bundle nodes are completed when they have their output in registers
                if f"{node.unique_id}:json" not in job.registers:
                    completed = False
            elif node.ntype == objs.PlusScriptNodeType.UPDATE_VALUES:
                # UPDATE_VALUES nodes are completed when they have been processed
                # We check for the special :processed marker
                processed_key = f"{node.unique_id}:processed"
                if processed_key not in job.registers:
                    completed = False
            elif node.ntype == objs.PlusScriptNodeType.UI_FEEDBACK:
                # UI_FEEDBACK nodes are completed when they have been processed
                # We check for the special :processed marker
                processed_key = f"{node.unique_id}:processed"
                if processed_key not in job.registers:
                    completed = False

        job.iteration += 1

        if completed:
            reverse_links = self._reverse_links(job.script.links)
            job.status = objs.PlusScriptStatus.SUCCEEDED
            main = reverse_links.get('mainOutput', {})
            for output_param in job.script.outputs:
                var_name = output_param.var_name
                if var_name not in main:
                    continue
                link = main[var_name]
                src = f"{link.source}:{link.sourceHandle}"
                at = job.registers[src]
                job.output[var_name].CopyFrom(at)
            
            # Print final state for UPDATE_VALUES and UI_FEEDBACK nodes
            update_value_registers = {k: v for k, v in job.registers.items() if ":update_value:" in k}
            ui_feedback_registers = {k: v for k, v in job.registers.items() if any(
                k.startswith(f"{node.unique_id}:") and ":update_value:" not in k and ":processed" not in k
                for node in job.script.nodes if node.ntype == objs.PlusScriptNodeType.UI_FEEDBACK
            )}
            
            if update_value_registers:
                print(f"[PSEE] Job completed with {len(update_value_registers)} update_value registers:")
                for key, value in update_value_registers.items():
                    param_name = key.split(":update_value:")[-1]
                    print(f"[PSEE] Final update_value: {param_name} = {util.any_type_resolved(value)} (type: {value.ptype})")
            else:
                print("[PSEE] Job completed with no update_value registers")
            
            if ui_feedback_registers:
                print(f"[PSEE] Job completed with {len(ui_feedback_registers)} ui_feedback registers:")
                for key, value in ui_feedback_registers.items():
                    print(f"[PSEE] Final ui_feedback: {key} = {util.any_type_resolved(value)} (type: {value.ptype})")
            else:
                print("[PSEE] Job completed with no ui_feedback registers")

        return job

    def should_run_remotely(self, sys_name):
        
        if sys_name in ['ecs-1', 'feaas-discord']:
            return (True, sys_name)
        
        else:
            return (False, None)