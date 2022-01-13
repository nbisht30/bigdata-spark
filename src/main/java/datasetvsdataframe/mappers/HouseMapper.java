package datasetvsdataframe.mappers;

import datasetvsdataframe.pojos.House;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Row;

import java.text.SimpleDateFormat;

public class HouseMapper implements MapFunction<Row, House> {

    /**
     *
     */
    private static final long serialVersionUID = -2L;


    @Override
    public House call(Row value) throws Exception {

        House h = new House();

        h.setId(value.getAs("id"));
        h.setAddress(value.getAs("address"));
        h.setSqft(value.getAs("sqft"));
        h.setPrice(value.getAs("price"));

        String vacancyDateString = value.getAs("vacantBy").toString();

        if (vacancyDateString != null) {
            SimpleDateFormat parser = new SimpleDateFormat("yyyy-mm-dd");
            h.setVacantBy(parser.parse(vacancyDateString));
        }

        return h;

    }

}