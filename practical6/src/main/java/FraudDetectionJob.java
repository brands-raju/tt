import java.io.Serializable;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class FraudDetectionJob {

    public static class Transaction implements Serializable {

        public String customerId;
        public double amount;
        public long timestamp;

        public Transaction() {}

        public Transaction(String customerId, double amount, long timestamp) {
            this.customerId = customerId;
            this.amount = amount;
            this.timestamp = timestamp;
        }
    }

    public static class FraudDetector
            extends KeyedProcessFunction<String, Transaction, String> {

        private transient ValueState<Transaction> previousTransaction;

        @Override
        public void open(Configuration parameters) throws Exception {

            ValueStateDescriptor<Transaction> descriptor =
                    new ValueStateDescriptor<>(
                            "previous-transaction",
                            Types.POJO(Transaction.class));

            previousTransaction =
                    getRuntimeContext().getState(descriptor);
        }

        @Override
        public void processElement(
                Transaction tx,
                Context ctx,
                Collector<String> out) throws Exception {

            if (tx.amount < 10000) {
                return;
            }

            Transaction previous = previousTransaction.value();

            if (previous != null) {

                long gap = tx.timestamp - previous.timestamp;

                if (gap >= 0 && gap <= 60000) {

                    out.collect(
                            "FRAUD ALERT: Customer=" + tx.customerId
                            + ", Previous=Rs." + previous.amount
                            + ", Current=Rs." + tx.amount
                            + ", Gap=" + gap / 1000 + " seconds");
                }
            }

            previousTransaction.update(tx);
        }
    }

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();

        long base = System.currentTimeMillis();

        DataStream<Transaction> transactions = env.fromData(
                new Transaction("C101", 15000, base + 10000),
                new Transaction("C202", 5000, base + 20000),
                new Transaction("C101", 20000, base + 45000),
                new Transaction("C202", 12000, base + 50000),
                new Transaction("C101", 3000, base + 55000),
                new Transaction("C101", 18000, base + 90000),
                new Transaction("C202", 14000, base + 130000)
        );

        transactions
                .keyBy(tx -> tx.customerId)
                .process(new FraudDetector())
                .print();

        env.execute("Fraud Detection Job");
    }
}
