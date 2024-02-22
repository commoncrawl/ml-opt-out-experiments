import ujson as json

from sparkcc import CCSparkJob

class UserAgentCount(CCSparkJob):
    """ Count server names sent in HTTP response header
        (WARC and WAT is allowed as input)"""

    name = "CountUserAgent"
    fallback_user_agent_name = '(no UserAgent specified in robots.txt file)'

    def process_record(self, record):
        if self.is_response_record(record):
            if record.rec_type == 'response' and record.http_headers and record.content_stream():
                # Check if the content type is 'text/plain'
                content_type = record.http_headers.get_header('Content-Type')
                if content_type and 'text/plain' in content_type:
                    # Extract content from the record
                    content = record.content_stream().read().decode('latin-1', errors='replace')

                    # Find and print the 'User-agent' directives
                    user_agent_lines = [line.strip() for line in content.splitlines() if line.startswith('User-agent:')]
                   
                    for user_agent_line in user_agent_lines:
                        user_agent = user_agent_line[len('User-agent:'):].strip()
                        yield user_agent, 1
        else:
            # warcinfo, request, non-WAT metadata records
            pass


if __name__ == "__main__":
    job = UserAgentCount()
    job.run()
