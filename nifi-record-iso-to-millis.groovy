import java.time.Instant
import org.apache.nifi.serialization.record.Record

Record transform(Record record) {
    def occurred = record.getValue('occurred_at')
    if (occurred instanceof CharSequence) {
        record.setValue('occurred_at', Instant.parse(occurred.toString()).toEpochMilli())
    }
    def ingest = record.getValue('ingest_ts')
    if (ingest instanceof CharSequence) {
        record.setValue('ingest_ts', Instant.parse(ingest.toString()).toEpochMilli())
    }
    return record
}
