package com.tank;

import com.tank.state.AccumulationStream;
import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import javax.annotation.Nonnull;
import java.util.*;

import static java.lang.String.format;

/**
 * @author tank198435163.com
 */
public class App {

  public static void main(final String[] args) throws Exception {
    final AccumulationStream accumulationStream = new AccumulationStream();
    accumulationStream.processIntegerStream("localhost", 7777);
  }

  private static void processDifferentTypeData() throws Exception {
    StreamExecutionEnvironment env = createStreamEnv();
    final String activityType = "activityTag";
    final String orderType = "orderTag";
    final String customerType = "customerTag";
    OutputTag<Tuple2<String, Integer>> activityTag = new OutputTag(activityType, TypeInformation.of(new TypeHint<Tuple2<String, Integer>>() {
    }));
    OutputTag<Tuple2<String, Integer>> orderTag = new OutputTag(orderType, TypeInformation.of(new TypeHint<Tuple2<String, Integer>>() {
    }));
    OutputTag<Tuple2<String, Integer>> customerTag = new OutputTag(customerType, TypeInformation.of(new TypeHint<Tuple2<String, Integer>>() {
    }));
    SingleOutputStreamOperator<Tuple2<String, Integer>> ds = env.fromElements(
            Tuple2.of("activityTag", 1),
            Tuple2.of("order", 2), Tuple2.of("customer", 3))
            .keyBy(0)
            .process(new KeyedProcessFunction<Tuple, Tuple2<String, Integer>, Tuple2<String, Integer>>() {
              @Override
              public void processElement(Tuple2<String, Integer> input, Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
                String dataType = input.f0;
                if (activityType.equalsIgnoreCase(dataType)) {
                  ctx.output(activityTag, input);
                } else if (orderType.equalsIgnoreCase(dataType)) {
                  ctx.output(orderTag, input);
                } else if (customerTag.equals(dataType)) {
                  ctx.output(customerTag, input);
                }
              }
            })
            .setParallelism(Runtime.getRuntime().availableProcessors());

    ds.getSideOutput(activityTag).print();
    ds.getSideOutput(orderTag).print();
    ds.getSideOutput(customerTag).print();

    env.executeAsync("tuple job").getJobStatus().whenComplete((job, exp) -> {
      System.out.println("args = " + job.name());
    });
  }

  private static void richMapping() throws Exception {
    StreamExecutionEnvironment env = createStreamEnv();
    env.fromElements(1, 2, 3).map(new RichMapFunction<Integer, Integer>() {

      @Override
      public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        System.out.println("once");
      }

      @Override
      public Integer map(Integer value) throws Exception {
        return value + 1;
      }

    }).setParallelism(1).print();

    env.executeAsync("richMapping");
  }

  private static StreamExecutionEnvironment createStreamEnv() {
    return StreamExecutionEnvironment.getExecutionEnvironment();
  }

  private static void splitStream() throws Exception {
    final StreamExecutionEnvironment env = createStreamEnv();
    final OutputTag<Integer> odd = new OutputTag<>("odd", TypeInformation.of(new TypeHint<Integer>() {
    }));
    final OutputTag<Integer> even = new OutputTag<>("even", TypeInformation.of(new TypeHint<Integer>() {
    }));

    SingleOutputStreamOperator<Integer> ds = env.fromElements(1, 2, 3, 4)
            .process(new ProcessFunction<Integer, Integer>() {
              @Override
              public void processElement(Integer value, Context ctx, Collector<Integer> out) throws Exception {
                out.collect(value);
                if (value % 2 == 0) {
                  ctx.output(odd, value);
                } else {
                  ctx.output(even, value);
                }
              }
            });

    ds.getSideOutput(odd).print();


    final JobClient jobClient = env.executeAsync("split stream job");
    jobClient.getJobStatus().whenComplete(((jobStatus, ex) -> {
      if (Objects.isNull(ex)) {
        System.out.println(format("job status:[%s]", jobStatus.name()));
      }
    }));
  }

  private static void reduceNumber() throws Exception {
    StreamExecutionEnvironment env = createStreamEnv();
    env.setParallelism(1);
    env.fromElements(1, 2, 3, 4)
            .map(new MapFunction<Integer, Tuple2<Integer, Integer>>() {
              @Override
              public Tuple2<Integer, Integer> map(Integer value) throws Exception {
                return Tuple2.of(value, 1);
              }
            })
            .keyBy(new KeySelector<Tuple2<Integer, Integer>, Integer>() {
              @Override
              public Integer getKey(Tuple2<Integer, Integer> value) throws Exception {
                return value.f1;
              }
            })
            .reduce(new ReduceFunction<Tuple2<Integer, Integer>>() {
              @Override
              public Tuple2<Integer, Integer> reduce(Tuple2<Integer, Integer> value1, Tuple2<Integer, Integer> value2) throws Exception {
                Tuple2<Integer, Integer> result = new Tuple2<>(value1.f0 + (value2.f0 == null ? 0 : value2.f0), value1.f1);
                return result;
              }
            })
            .map(new MapFunction<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>>() {
              @Override
              public Tuple2<Integer, Integer> map(Tuple2<Integer, Integer> value) throws Exception {
                return Tuple2.of(value.f1, value.f0);
              }
            })
            .print();


    final JobClient jobClient = env.executeAsync("reducer job");
    jobClient.getJobStatus()
            .whenComplete(((jobStatus, ex) -> {
              if (ex == null) {
                System.out.println(format("reducer job:[%s]", jobStatus.name()));
              }
            }));
  }

  private static void processBoundStream() throws Exception {
    StreamExecutionEnvironment env = createStreamEnv();
    env.getConfig().disableGenericTypes();
    String filePath = locateTextFilePath();
    env.readTextFile(filePath)
            .flatMap(new LineSplitter())
            .keyBy(0)
            .sum(1)
            .print();

    JobClient jobClient = env.executeAsync("word counter");
    jobClient.getJobStatus().whenCompleteAsync(((jobStatus, throwable) -> {
      System.out.println(format("job status name:%s", jobStatus.name()));
    }));
  }

  private static void processWordWithBoundStream() throws Exception {
    ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
    String path = locateTextFilePath();

    env.readTextFile(path)
            .flatMap(new LineSplitter())
            .groupBy(0)
            .sum(1)
            .print();
  }

  private static String locateTextFilePath() {
    return App.class.getClassLoader()
            .getResource("word.txt")
            .getPath();
  }

  private static class LineSplitter implements FlatMapFunction<String, Tuple2<String, Integer>> {
    @Override
    public void flatMap(String line, Collector<Tuple2<String, Integer>> out) {
      for (String word : line.split(" ")) {
        out.collect(new Tuple2<String, Integer>(word, 1));
      }
    }
  }

  private static class RichLineSplitter extends RichFlatMapFunction<String, Tuple2<String, Integer>> {

    @Override
    public void flatMap(@Nonnull final String line,
                        @Nonnull final Collector<Tuple2<String, Integer>> out) throws Exception {
      boolean isNotOk = Objects.isNull(line) || line.trim().length() == 0;
      if (isNotOk) {
        throw new Exception("line not allowed empty");
      }
      String[] arr = line.split(" ");
      for (String word : arr) {
        Tuple2<String, Integer> tuple2 = Tuple2.of(word, 1);
        out.collect(tuple2);
      }
    }

  }

  private static class SimpleWordSplitter implements FlatMapFunction<String, Tuple1<String>> {

    @Override
    public void flatMap(String in, Collector<Tuple1<String>> out) throws Exception {
      final String[] arr = in.split(" ");
      for (String word : arr) {
        out.collect(Tuple1.of(word));
      }
    }
  }

  private static class RichWord2Tuple extends RichMapFunction<Tuple1<String>, Tuple2<String, Integer>> {

    @Override
    public Tuple2<String, Integer> map(Tuple1<String> in) throws Exception {
      if (this.valueState != null) {
        Integer value = this.valueState.value();
        int result = value == null ? 1 : value;
        result++;
        this.valueState.update(result);
      }
      return Tuple2.of(in.f0, 1);
    }

    @Override
    public void open(Configuration parameters) throws Exception {
      ValueStateDescriptor<Integer> valueStateDescriptor = new ValueStateDescriptor<Integer>("wcs", Integer.class);
      this.valueState = this.getRuntimeContext().getState(valueStateDescriptor);
    }

    private ValueState<Integer> valueState;
  }
}
