import pkgutil
import inspect
import pyclbr
from importlib import import_module
from google.protobuf.json_format import Parse, MessageToDict
import logging
import traceback

class AvailableActionCrawler(object):


    def __init__(self, sys_name, dao, module_prefix, owner = 'sys.action'):
        self.sys_name = sys_name
        self.dao = dao
        self.module_prefix = module_prefix
        self.owner = owner
        self.cache = None
        self.failed_count = 0


    def get_actions(self):
        actions = self._run_for_path(self.module_prefix)
        n = len(actions)
        allresults = []
        for action in actions:
            module_path = action.replace('/', '.')
            results = self._crawl_module(module_path)
            allresults.extend(results)
        print(f"Found {len(allresults)} actions and {self.failed_count} failures")
        return allresults


    def _run_for_path(self, module_prefix):
        results = []
        try:
            items = list(pkgutil.iter_modules(path=[module_prefix]))
        except:
            print(traceback.format_exc())
            print(f"Failed on {module_prefix}")
            items = []
        for item in items:
            if item.module_finder.path.endswith(module_prefix):
                item_path = module_prefix + '/' + item.name
                if item.ispkg:
                    results.extend(self._run_for_path(item_path))
                else:
                    results.append(item_path)

        return results


    def _crawl_module(self, module_path):
        results = []
        ignoreList = [
            'src.actions.vendor.aws.athena.athena_helper.AthenaTableBuilder',
            'src.actions.vendor.aws.athena.create.AthenaTableBuilderSub',
            'src.actions.vendor.aws.athena_osm.osm.AthenaQueryRunner',
            'src.actions.vendor.neo4j.load.Neo4jConnection',
            'src.actions.vendor.neo4j.loadimdb.Neo4jConnection',
            'src.actions.client.neonpixel.optimizer.NeonPixelOptimizer',
            'chalicelib.actions.agent.eliza.eliza.Eliza',
            'chalicelib.actions.web.crawler.utils.CrawlHelper',
            'chalicelib.actions.counters.counter.Counter',
            'src.workers.worker_thread.WorkerThread'
        ]

        try:
            class_dict = pyclbr.readmodule(module_path)
        except:
            logging.warning(f"*** WARNING: NO CLASSES FOUND IN {module_path}")
            return results

        try:
            module = import_module(module_path)
        except Exception:
            logging.error(traceback.format_exc())
            logging.error(f"Failed to import {module_path}")
            self.failed_count += 1
            return results

        from feaas.abstract import AbstractAction  # import here to avoid circular issues

        for class_name in class_dict.keys():
            full_class_path = f"{module_path}.{class_name}"
            if full_class_path in ignoreList:
                logging.debug(f"Ignoring {full_class_path}")
                continue

            try:
                klass = getattr(module, class_name)
            except AttributeError:
                logging.warning(f"Class {class_name} not found in {module_path}")
                continue

            # Check for AbstractAction inheritance
            try:
                if not issubclass(klass, AbstractAction):
                    continue
                if inspect.isabstract(klass):
                    continue
            except TypeError:
                continue  # klass isn't a class at all

            try:
                action_instance = klass(dao=self.dao)
                action_instance.action.sys_name = self.sys_name
                action_json = MessageToDict(
                    action_instance.action,
                    preserving_proto_field_name=True,
                    always_print_fields_with_no_presence=True
                )
                action_json['owner'] = self.owner
                action_json['sys_name'] = self.sys_name
                action_json['class'] = klass  
                results.append(action_json)
            except Exception as e:
                logging.error(f"Error instantiating {full_class_path}: {traceback.format_exc()}")
                self.failed_count += 1

        return results
