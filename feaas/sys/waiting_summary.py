# from feaas.util import common

# from datetime import datetime, timezone
# import time
# import json

#     def execute_work(self, work: Request) -> list:
#         import pandas as pd
#         from io import StringIO
#         src_prefix = work.get_src_prefix()
#         if src_prefix.endswith('/'):
#             src_prefix = src_prefix[:-1]
#         s3_list = list(self.blobstore.s3list(src_prefix))
#         waiting_list = [s3_list[i][0] for i in range(1, len(s3_list)) if s3_list[i][0].endswith(".waiting")]
#         if len(waiting_list) == 0:
#             df = pd.DataFrame(columns=["key", "age", "dest_ext"])
#         else:
#             out_list = []
#             for file in waiting_list:
#                 metadata = self.blobstore.get_blob_metadata(file)
#                 o = {
#                     "key": file,
#                     "age": int((datetime.now(timezone.utc) - metadata["last_modified"]).total_seconds()),
#                     "dest_ext": common.get_extension(file).rsplit(".", 1)[0]
#                 }
#                 out_list.append(o)
#             df = pd.DataFrame(out_list)
#         buff = StringIO()
#         df.to_csv(buff, index=False)
#         now = datetime.now(timezone.utc)
#         date_string = now.strftime("%Y-%m-%d-%H")
#         log_key = f"sys/waiting/logs/dt={date_string}/waiting_summary.csv"
#         self.blobstore.save_blob(log_key, buff.getvalue().encode())
#         feed_id, published_at = self._analyze_waiting_summary(df)
#         return [Receipt(feed_id, published_at)]


#     def _analyze_waiting_summary(self, df):
#         import numpy as np
#         feed_id = "com.dataskeptic.portal.waiting"
#         published_at = int(time.time() * 1000)
#         out = {}
#         out["published_at"] = published_at
#         out["num_files"] = df.shape[0]
#         if out["num_files"] > 0:
#             out["avg_wait_time"] = int(df["age"].mean())
#             out["max_wait_info"] = json.loads(df.loc[df["age"].argmax()].to_json())
#             out["quantiles"] = []
#             quants = json.loads(df["age"].quantile(np.linspace(.25, 1, 4)).to_json())
#             for k, v in quants.items():
#                 inner = {}
#                 inner["percentile"] = k
#                 inner["value"] = int(v)
#                 out["quantiles"].append(inner)
#             out["most_common_exts"] = []
#             exts = json.loads(df["dest_ext"].value_counts()[:5].to_json())
#             for k, v in exts.items():
#                 inner = {}
#                 inner["extension"] = k
#                 inner["frequency"] = int(v)
#                 out["most_common_exts"].append(inner)
#         self.feeds.save_document(feed_id, out)
#         return feed_id, published_at
