package woohoo;


import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.formats.json.JsonNodeDeserializationSchema;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import org.apache.flink.util.Collector;

import java.util.Properties;

/**
 * Count the number of bot and non-bot changes being made in the wikimedia
 * stream, which is expected to be in a topic named wikiedits in a local Kafka
 * cluster.  Either output that number to the screen, or as a json-encoded
 * record to another Kafka topic named botcounts.
 */
public class BotCount  {
    public static void main( String[] args ) throws Exception {
        // Parse the command line args to figure out what to output
        ParameterTool parameter = ParameterTool.fromArgs(args);
        System.out.println("args length = " + args.length);
        System.out.println("arg[0] = " + args[0]);
        System.out.println("parameter = " + parameter.get("output"));
        String output = parameter.get("output", "terminal");
        System.out.println("output = " + output);
        if(!(output.equals("terminal") || output.equals("kafka"))) {
            throw new IllegalArgumentException("Need to write output either to the terminal or to Kafka, not \" + output");
        }

        // Required Kafka properties
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Create a source for the Kafka topic and treat its entries as
        // json, which we'll interact with as ObjectNodes - really crappy json wrappers
        FlinkKafkaConsumer011<ObjectNode> wikieditsKafkaTopic
            = new FlinkKafkaConsumer011<>("wikiedits", new JsonNodeDeserializationSchema(), properties);
        // Start reading from the latest entries
        wikieditsKafkaTopic.setStartFromLatest();

        // Add the stream into the Filnk job
        DataStream<ObjectNode> wikiedits = env.addSource(wikieditsKafkaTopic);

        // For each record in wikiedits...
        SingleOutputStreamOperator<Tuple2<Boolean, Integer>> botCounts = wikiedits
            // Extract out the bot field, casting it to a boolean, and build a tuple of that value and a count of 1
            // ie. <true, 1> or <false, 1>
            .map(edit -> new Tuple2<>(edit.get("bot").asBoolean(), 1))
            // Java loses type information, so we have to tell Flink what we got otu of the map.
            // Scala doesn't have this problem... just sayin'.
            .returns(TypeInformation.of(new TypeHint<Tuple2<Boolean, Integer>>() {}))
            // Group the result stream by the boolean field (position 0)
            .keyBy(0)
            // Window the results into windows that spit them out every 10 seconds
            .timeWindow(Time.seconds(10))
            // And sum of the count of trues and falses in each window
            .sum(1);

        if(output.equals("terminal")) {
            System.out.println("Writing results to the terminal");
            // Every 10 seconds we get
            // 5> (true,55)
            // 1> (false,66)
            // or whatever the results were.  The 5> and 1> above are Flink's
            // internal processors numbers.
            botCounts.print();
        } else {
            System.out.println("Writing results to kafka topic botcount");
            // When we print out to the screen, it's obvious which counts
            // belong to each window since they appear at the same time.
            // When we write the results to a Kafka topic, we should group
            // the results into a single value, which we'll encode in json.

            // Here, we take the two elements from the timeWindow before (the
            // count of true and count of false) and transform into a single
            // json record with both values.  This record is what's then
            // written to the output Kafka topic.
            SingleOutputStreamOperator<String> botCountsAsJson = botCounts
                .countWindowAll(2)
                .apply(
                    // Java 8 doesn't store type info in lambdas, so we must use an anonymous function here.
                    new AllWindowFunction<Tuple2<Boolean, Integer>, String, GlobalWindow>() {
                        @Override
                        // For those two values we got from the window, grab the true count and false count
                        // then concatenate them into a json datum and output that
                        public void apply(GlobalWindow window, Iterable<Tuple2<Boolean, Integer>> values, Collector<String> out) throws Exception {
                            int trueCount = 0;
                            int falseCount = 0;

                            for (Tuple2<Boolean, Integer> value : values) {
                                if (value.f0) {
                                    trueCount = value.f1;
                                } else {
                                    falseCount = value.f1;
                                }
                            }

                            out.collect(String.format("{ 'true': %d, 'false': %d}", trueCount, falseCount));
                        }
                    }
                );

            // Create a producer for the botcount topic
            FlinkKafkaProducer011<String> botcountTopic = new FlinkKafkaProducer011<>(
                    "localhost:9092",            // broker list
                    "botcount",                  // target topic
                    new SimpleStringSchema());   // serialization schema

            // Wire the botCountsAsJson stream to the output topic
            botCountsAsJson.addSink(botcountTopic);
        }

        env.execute();
    }
}
