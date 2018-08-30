# -*- coding: utf-8 -*-
"""
A word-counting workflow.
"""

from __future__ import absolute_import

import argparse
import logging

import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import Read
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions


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
  p = beam.Pipeline(options=pipeline_options) #in→text out→textのパイプライン
  p2 = beam.Pipeline(options=pipeline_options) #in→bigquery out→textのパイプライン

  ##############################################
  # (2) transformを設定
  ###############################################

  #pにtransformを設定
  lines = p | 'read' >> ReadFromText(known_args.input)
  #ファイル出力するためのサンプルメソッド
  def add(line):
      num = int(line.strip())
      return num *3
  counts = lines | 'add' >> beam.Map(add)
  counts | 'write' >> WriteToText(known_args.output)

  #p2にtransformを設定
  query = 'select * from babynames.names2012 limit 1000'
  p2 | 'read' >> Read(beam.io.BigQuerySource(project='gcp-project-210712', use_standard_sql=False, query=query)) \
     | 'write' >> WriteToText('gs://gcp_dataflowsample/query_result.txt', num_shards=1)

  ###############################################
  # (3) Pipeline を実行
  ###############################################

  #result = p.run()
  result2 = p2.run()

  # 終了を待つ
  # 記述しなければそのまま抜ける
  # →DataFlowRunnerの場合、Ctrl-Cでもパイプラインは停止しない。Gooleコンソールから停止する必要がある
  #ここで結果が終了するのを待ち合わせている。記載がなければ後続は処理されない。
  #result.wait_until_finish()
  result2.wait_until_finish()



if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()