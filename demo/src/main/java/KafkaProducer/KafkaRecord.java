package KafkaProducer;

//the record information has to be instance of KafkaRecord
public interface KafkaRecord<K, V> {

    public String getKeyAsString();

    public String getValueAsString();

    public K getKey();

    public V getValue();

}
