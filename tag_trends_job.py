from mrjob.job import MRJob
from mrjob.protocol import JSONValueProtocol
import xml.etree.ElementTree as ET
import re
import time
import json


class MRTagTrend(MRJob):
    OUTPUT_PROTOCOL = JSONValueProtocol

    def from_xml(self, line):
        return ET.fromstring(line)

    #the mapper groups by tag, it gets a posted item as line, if the item is a question it emits all tags 
    #as keys 1 as the value
    def mapper(self, _, line):
      TAG_RE = re.compile('<([^<>]*)>')
      try:
          line_xml  = self.from_xml(line)
          if line_xml.get('PostTypeId') == '1':
              posted_time = line_xml.get("CreationDate")
              posted_time = time.strptime(posted_time,  "%Y-%m-%dT%H:%M:%S.%f")
              posted_time = time.strftime("%Y-%m-01", posted_time)
              for tag in TAG_RE.findall(line_xml.get('Tags')):
                  yield ((tag,posted_time), 1)
      except Exception:
          return

    #the reducer gets a tag as key and a list of 1's the number of questions for that tag and emits the sum
    def reducer(self, key, values):
        #used json.dumps to get a friendly output easier to read and get into a database later
        yield key, json.dumps({"tag": key[0], "date": key[1], "count": sum(values)})


if __name__ == '__main__':
    MRTagTrend.run()
