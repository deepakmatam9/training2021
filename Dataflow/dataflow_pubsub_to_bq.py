import csv
import time
import logging
from datetime import datetime

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.transforms import window, trigger
from apache_beam.metrics import Metrics


class PubsubToBQOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        #parser.add_argument('--topic', help='Pubsub topic to read data from', required=True)
        #parser.add_argument('--target_table', help='Target BQ table', required=True)
        group = parser.add_mutually_exclusive_group(required=True)
        group.add_argument('--topic', help='Pubsub topic to read data from. Not requied if subscription is provided')
        group.add_argument('--subscription', help='Pubsub subscription to read data. Not required if topic is provided')


def timestamp2str(t, fmt='%Y-%m-%d %H:%M:%S.000'):
  """Converts a unix timestamp into a formatted string."""
  return datetime.fromtimestamp(t).strftime(fmt)


def format_user_score_sums(user_score):
    (user, score) = user_score
    return {'user': user, 'total_score': score}


class ParseGameEvenFn(beam.DoFn):
    #username,teamname,score,timestamp_in_ms,readable_time
    def __init__(self):
        beam.DoFn.__init__(self)
        self.num_parse_errors = Metrics.counter(self.__class__, 'num_parse_errors')

    def process(self, element):
        try:
            row = list(csv.reader([element]))[0]
            yield {
                'user': row[0],
                'team': row[1],
                'score': int(row[2]),
                'timestamp': int(row[3])/1000.0
            }
        except:
            self.num_parse_errors.inc()
            logging.error('Parse error on "%s"', element)


class ExtractAndSumScore(beam.PTransform):
    def __init__(self, field):
        beam.PTransform.__init__(self)
        self.field = field

    def expand(self, input_pcol):
        return (
            input_pcol
            | beam.Map(lambda elem: (elem[self.field],elem['score']))
            | beam.CombinePerKey(sum)
        )

class TeamScoresDict(beam.DoFn):
    def process(self, team_score, window=beam.DoFn.WindowParam):
        team, score = team_score
        start = timestamp2str(int(window.start))
        yield {
            'team': team,
            'total_score': score,
            'window_start': start,
            'processing_time': timestamp2str(int(time.time()))
        }


class WriteToBigQuery(beam.PTransform):
    def __init__(self, table_name, dataset, schema, project):
        beam.PTransform.__init__(self)
        self.table_name = table_name
        self.dataset = dataset
        self.schema = schema
        self.project = project

    def get_schema(self):
        return ', '.join('%s:%s' % (col, self.schema[col]) for col in self.schema)

    def expand(self, input_pcol):
        return (
            input_pcol
            | 'ConvertToRow' >> beam.Map(lambda elem: {col: elem[col] for col in self.schema})
            | 'WriteToBigQuery' >> beam.io.WriteToBigQuery(self.table_name, self.dataset, self.project, self.get_schema())
        )


class CalculateTeamScores(beam.PTransform):
    def __init__(self, team_window_duration, allowed_lateness):
        beam.PTransform.__init__(self)
        self.team_window_duration = team_window_duration * 60
        self.allowed_lateness_seconds = allowed_lateness * 60

    def expand(self, input_pcol):
        return (
            input_pcol
            | 'LeaderboardTeamFixedWindows' >> beam.WindowInto(window.FixedWindows(self.team_window_duration),
                                                               trigger=trigger.AfterWatermark(trigger.AfterCount(10),trigger.AfterCount(20)),
                                                               accumulation_mode=trigger.AccumulationMode.ACCUMULATING)
            | 'ExtractAndSumScore' >> ExtractAndSumScore('team')
        )


class CalculateUserScores(beam.PTransform):
    def __init__(self, allowed_lateness):
        beam.PTransform.__init__(self)
        self.allowed_lateness_seconds = allowed_lateness * 60

    def expand(self, input_col):
        return (
            input_col
            | 'LeaderboardUserGlobalWindows' >> beam.WindowInto(window.GlobalWindows(), trigger=trigger.Repeatedly(trigger.AfterCount(10)),
                                                                accumulation_mode=trigger.AccumulationMode.ACCUMULATING)
            | 'ExtractAndSumScore' >> ExtractAndSumScore('user')
        )



def run(argv=None, save_main_session=True):
    pipeline_options = PubsubToBQOptions()
    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session
    pipeline_options.view_as(StandardOptions).streaming = True

    project_id = pipeline_options.view_as(GoogleCloudOptions).project
    topic = None
    subscription = None
    if pipeline_options.topic:
        topic = f"projects/{project_id}/topics/{pipeline_options.topic}"
        subscription = None
    if pipeline_options.subscription:
        topic = None
        subscription = f"projects/{project_id}/subscriptions/{pipeline_options.subscription}"

    with beam.Pipeline(options=pipeline_options) as p:
        scores = p | 'ReadFromPubsub' >> beam.io.ReadFromPubSub(topic=topic,subscription=subscription)
        events = (
            scores
            | 'DecodeString' >> beam.Map(lambda b: b.decode('utf-8'))
            | 'ParseGameEventFn' >> beam.ParDo(ParseGameEvenFn())
            | 'AddEventTimestamps' >> beam.Map(lambda elem: window.TimestampedValue(elem,elem['timestamp']))
        )
        (
            events
            | 'CalculateTeamScores' >> CalculateTeamScores(team_window_duration=30, allowed_lateness=10)
            | 'TeamScoresDict' >> beam.ParDo(TeamScoresDict())
            | 'WriteTeamScoreSums' >> WriteToBigQuery(
                pipeline_options.table_name + '_teams',
                pipeline_options.dataset,
                {
                    'team': 'STRING',
                    'total_score': 'INTEGER',
                    'window_start': 'STRING',
                    'processing_time': 'STRING',
                },
                pipeline_options.view_as(GoogleCloudOptions).project)
        )
        (
            events
            | 'CalculateUserScores' >> CalculateUserScores(10)
            | 'FormatUserScoreSums' >> beam.Map(format_user_score_sums)
            | 'WriteUserScoreSums' >> WriteToBigQuery(
                pipeline_options.table_name + '_users',
                pipeline_options.dataset,
                {
                    'user': 'STRING',
                    'total_score': 'INTEGER',
                },
                pipeline_options.view_as(GoogleCloudOptions).project)
        )


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()

