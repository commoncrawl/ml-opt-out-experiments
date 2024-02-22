import ujson as json

from sparkcc import CCSparkJob

class HttpHeaderCount(CCSparkJob):
    """ Count HTTP headers. WARC is allowed."""

    name = "CountHttpHeaders"
    PROXY_HEADER = "proxy_for_num_responses"
    def process_record(self, record):
        if self.is_response_record(record):
            if record.rec_type == 'response' and record.http_headers and record.http_headers.headers and record.content_stream():
                yield self.PROXY_HEADER, 1
                header_list = [header[0] for header in record.http_headers.headers]
                for header in header_list:
                    yield header, 1
        else:
            # warcinfo, request, non-WAT metadata records
            pass


if __name__ == "__main__":
    job = HttpHeaderCount()
    job.run()
