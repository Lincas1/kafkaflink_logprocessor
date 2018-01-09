package custom_serializer;




import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Map;

public class CustomerSerializer implements Serializer<Customer> {

    @Override
    public void configure(Map configs, boolean isKey) {
        // nothing to configure
    }

    @Override
    /**
     We are serializing Customer as:
     4 byte int representing customerId
     4 byte int representing length of customerName in UTF-8 bytes (0 if name is Null)
     N bytes representing customerName in UTF-8
     */
    public byte[] serialize(String topic, Customer data) {
        try {
            byte[] serializedName;
            int stringSize;
            if (data == null)
                return null;
            else {
                if (data.getCustomerName() != null) {
                    serializedName = data.getCustomerName().getBytes("UTF-8");
                    stringSize = serializedName.length;
                } else {
                    serializedName = new byte[0];
                    stringSize = 0;
                }
            }

            ByteBuffer buffer = ByteBuffer.allocate(4 + 4 + stringSize);
            buffer.putInt(data.getID());
            buffer.putInt(stringSize);
            buffer.put(serializedName);

            return buffer.array();
        } catch (Exception e) {
            throw new SerializationException("Error when serializing Customer to byte[] " + e);
        }
    }

    @Override
    public void close() {
        // nothing to close
    }


    public static void main(String[] args) {
        CustomerSerializer my = new CustomerSerializer();
        byte[] res = my.serialize("123", new Customer(123, "i am a handsome boy"));
        System.out.println(Arrays.toString(res));
        res = my.serialize("123", new Customer(123, "i am a handsome boy, oh yeah"));
        System.out.println(Arrays.toString(res));
    }
}
