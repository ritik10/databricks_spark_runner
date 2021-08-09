from databricks_api.databricks import DatabricksAPI as db
from databricks_api import DatabricksAPI
import time
from environment_constants import DatabricksEnv as DBC

class DatabricksRunner:
    def __init__(self,host_id,access_token):
        self.databricks = DatabricksAPI(host=host_id, token=access_token)
        self.host_id = host_id
        self.access_token = access_token
        self.cluster_is_running = False
        self.cluster_is_defined = False
        self.job_client=self.databricks.jobs
        self.cluster_client=self.databricks.cluster

    def get_job_id(self, job_list, name):
        ret_val = None
        jobs = job_list.get("jobs", None)
        if jobs is not None:
            for i in range(len(jobs)):
                job_name = jobs[i].get("settings", {}).get("name", None)
                if name == job_name:
                    ret_val = jobs[i]
                    break
        return ret_val

    def connect_to_cluster(self, host_id, access_token, cluster_name, headers):
        self.host_id = host_id
        self.access_token = access_token
        clusters = self.cluster_client.list_clusters(headers=headers)
        self.cluster_is_running = False
        self.cluster_is_defined = False
        for c in clusters["clusters"]:
            if c["cluster_name"] == cluster_name and self.cluster_is_defined == False:
                self.cluster_is_defined = True
            if c["state"] == "RUNNING":
                self.cluster_is_running = True
            elif c["state"] == "TERMINATED":
                self.cluster_is_running = False
            else:
                self.cluster_is_running = False
            if self.cluster_is_running is False:
                return False
            if self.cluster_is_defined is False:
                return False
        return True

    def get_job_id(self, job_name):
        if not self.cluster_is_running:
            return None
        job_list = self.job_client.list_jobs(headers=None)
        job_details = self.get_job_id(job_list, job_name)
        if job_details is None:
            return None
        self.databricks_job_id = job_details["job_id"]
        return True



    def wait_for_job(self, poll_time, timeout_value):
        life_cycle_state = "RUNNING"
        run_time = 0
        result_state = ""
        while "TERMINATED" != life_cycle_state and run_time <= timeout_value:
            run_response = self.job_client.get_run(
                run_id=self.job_run_id, headers=None
            )
            state = run_response.get("state", None)
            if state is not None:
                life_cycle_state = state.get("life_cycle_state", None)
                result_state = state.get("result_state", None)
                time.sleep(poll_time)
                run_time = run_time + poll_time
        if run_time >= timeout_value:
            return "job is still running"
        elif "TERMINATED" == life_cycle_state:
            if result_state == "success":
                ret_val = "success"
            elif result_state == "failed":
                ret_val = "failed"
        return ret_val

    def get_job_list(self):
        return self.job_client.list_jobs()
    
    def create_job(self):
        pass
    def get_job_by_id(self):
        pass
    def get_job_by_name(self):
        pass
    def get_cluster_by_name(self):
        pass
    def is_cluster_up(self):
        pass
    def get_cluster_by_id(self):
        pass
    def run_job(self,job_id):
        self.job_client.run_now()
    def run_spark_job(
        self, notebook_params, python_params, spark_submit_params, headers
    ):
        ret_val = self.job_client.run_now(
                            job_id=self.databricks_job_id,
                            notebook_params=notebook_params,
                            python_params=python_params,
                            spark_submit_params=spark_submit_params,
                            headers=headers,
                            )
        self.job_run_id = ret_val["run_id"]
        return True
    def get_run_details(self,run_id):
        pass
if __name__ == '__main__':

    databricks=DatabricksRunner(DBC.HOST,DBC.TOKEN)
    print(databricks.get_job_list())