package io.matthewbrown.flink.bigquery;

import org.joda.time.DateTime;
import org.joda.time.DateTimeFieldType;
import org.joda.time.LocalDate;
import org.joda.time.LocalTime;

public class JodaConverter {
    private static JodaConverter instance;

    public static JodaConverter getInstance() {
        if (instance != null) {
            return instance;
        }

        instance = new JodaConverter();
        return instance;
    }

    public long convertDate(Object object) {
        final LocalDate value = (LocalDate) object;
        return value.toDate().getTime();
    }

    public int convertTime(Object object) {
        final LocalTime value = (LocalTime) object;
        return value.get(DateTimeFieldType.millisOfDay());
    }

    public long convertTimestamp(Object object) {
        final DateTime value = (DateTime) object;
        return value.toDate().getTime();
    }
}
