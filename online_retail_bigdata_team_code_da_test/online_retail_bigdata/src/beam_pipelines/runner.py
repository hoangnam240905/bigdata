from apache_beam.portability.api import beam_provision_api_pb2
from apache_beam.runners.portability.fn_api_runner import fn_runner
from apache_beam.runners.portability.portable_runner import JobServiceHandle


class MultiWorkerRunner(fn_runner.FnApiRunner):
    """FnApiRunner có gửi pipeline options sang worker con (như DirectRunner), để worker đọc/ghi được hdfs://."""

    def run_pipeline(self, pipeline, options):
        encoded = JobServiceHandle.encode_pipeline_options(options.get_all_options())
        self._provision_info = fn_runner.ExtendedProvisionInfo(
            beam_provision_api_pb2.ProvisionInfo(pipeline_options=encoded))
        return super().run_pipeline(pipeline, options)
