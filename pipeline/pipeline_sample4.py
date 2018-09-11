# -*- coding: utf-8 -*-
"""
pleline_sample(BigQuery to ModifyElement to  BigQuery)
"""

from __future__ import absolute_import

import argparse
import logging

import apache_beam as beam
from apache_beam.io import Read
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions

#加工処理の例(件数が10000以上の名前と件数を返す
def modify_data1(element):

  return {'name':element['name'].upper(),
                'count':element['count']}

# DoFn transform の実装例
def run(argv=None):
  """Main entry point; defines and runs the wordcount pipeline."""

  parser = argparse.ArgumentParser()
  parser.add_argument('--input',
                      dest='input',
                      default='gs://dataflow-samples/shakespeare/kinglear.txt',
                      help='Input file to process.')
  parser.add_argument('--output',
                      dest='output',
                      required=True,
                      help='Output file to write results to.')
  known_args, pipeline_args = parser.parse_known_args(argv)


  ###############################################
  # (1) pipeline を作成する
  ###############################################

  # まず PipelineOptions オブジェクトを作成
  # パイプラインを実行する pipeline runner や、選択した runner が必要とする固有の設定など、さまざまなオプションを設定できる
  pipeline_options = PipelineOptions(pipeline_args)

  # 作成した PipelineOptions オプジェクトを直接編集する例
  # 今回は DoFn transform を使用するため、save_main_sessionオプションを有効にする
  pipeline_options.view_as(SetupOptions).save_main_session = True

  # オプションを元に pipeline (p) を作成
  p4 = beam.Pipeline(options=pipeline_options) #in→bigquery out→textのパイプライン

  ##############################################
  # (2) transformを設定
  ###############################################

  #p2にtransformを設定
  query = 'select * from babynames.names2012'

  p4 | 'read' >> Read(beam.io.BigQuerySource(project='gcp-project-210712', use_standard_sql=False, query=query)) \
     | 'modify' >> beam.Map(modify_data1) \
     | 'write' >> beam.io.Write(beam.io.BigQuerySink( \
        'babynames.testtable2',schema='name:string, count:integer', \
        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
        write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE))

  ###############################################
  # (3) Pipeline を実行
  ##############################################)
  result4 = p4.run()

  # 終了を待つ
  # 記述しなければそのまま抜ける
  # →DataFlowRunnerの場合、Ctrl-Cでもパイプラインは停止しない。Gooleコンソールから停止する必要がある
  #ここで結果が終了するのを待ち合わせている。記載がなければ後続は処理されない。
  result4.wait_until_finish()



if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()