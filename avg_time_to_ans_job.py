from mrjob.job import MRJob
from mrjob.protocol import JSONValueProtocol
import xml.etree.ElementTree as ET
import re
import time
import json


class MRAvgTimeToAcceptedAnswer(MRJob):

    OUTPUT_PROTOCOL = JSONValueProtocol

    def steps(self):
        return [
            self.mr(mapper=self.mapper_get_answers,
                    reducer=self.reducer_join_answers),
            self.mr(reducer=self.reducer_calculate_time)
        ]

    def from_xml(self, line):
        return ET.fromstring(line)

    #the mapper groups by question_id, it will group a question with all of its answers.
    #for each posted question emits
    #    [question_id, [false, answer_id, posted_time, tags_for_that_question]
    #for each posted answer emits
    #    [question_id_the_answer_is_for, [true, answer_id, posted_time]
    def mapper_get_answers(self, _, line):
      try:
          line_xml  = self.from_xml(line)
          if line_xml.get('PostTypeId') == '1':
              id = line_xml.get('Id')
              posted_time = line_xml.get("CreationDate")
              acceptedAnswerId = line_xml.get('AcceptedAnswerId')
              tags = line_xml.get('Tags')
              yield (id, [False, acceptedAnswerId, posted_time, tags])
              return
          else:
              parentId = line_xml.get('ParentId')
              id = line_xml.get('Id')
              posted_time = line_xml.get("CreationDate")
              yield (parentId, [True, id, posted_time])
      except Exception as e:
          return

    #the reducer gets the question its tags and all of its posted answers, figures out the accepted answer
    #calculates the time difference between question and the accepted answer and emits the difference for each tag
    #in the question
    def reducer_join_answers(self, key, values):
        TAG_RE = re.compile('<([^<>]*)>')
        question = values.next()
        try:
            for ans in values:
                if(ans[1] == question[1]):
                    question_time = time.strptime(question[2],  "%Y-%m-%dT%H:%M:%S.%f")
                    answer_time = time.strptime(ans[2],  "%Y-%m-%dT%H:%M:%S.%f")
                    #time difference in number of days
                    diff = (time.mktime(answer_time) - time.mktime(question_time))/(60*60*24)
                    posted_time = time.strftime("%Y-%m-01", question_time)
                    for tag in TAG_RE.findall(question[3]):
                      yield ((tag,posted_time), diff)
        except Exception as e:
            return

    #the reducer gets tag as the key, and list of time differences for questions of that tag
    #it caluclates the avg difference and emits tag and the avg
    def reducer_calculate_time(self, key, values):
        total = 0.0
        num_questions = 0
        for diff in values:
            total+=diff
            num_questions+=1
        if num_questions != 0:
            yield ((key), (key[0], key[1], total/num_questions))


if __name__ == '__main__':
    MRAvgTimeToAcceptedAnswer.run()
