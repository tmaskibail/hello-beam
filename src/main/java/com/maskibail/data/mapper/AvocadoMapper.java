package com.maskibail.data.mapper;

import com.maskibail.data.model.AvocadoSale;
import org.apache.beam.sdk.transforms.DoFn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;

 public class AvocadoMapper extends DoFn<String, AvocadoSale> {
    private static final Logger LOG = LoggerFactory.getLogger(AvocadoMapper.class);

    @ProcessElement
    public void processElement(@Element String row, OutputReceiver<AvocadoSale> outputReceiver) {

        String[] fields = row.split(",");
        //Hack to skip the headers until a CSV reader in Beam is available
        //OR
        //Read CSVusing CSV libraries and then apply the data to the pipeline
        String concatColumns = Arrays.toString(fields);
        if (concatColumns.contains("AveragePrice")) {
            LOG.info("Skipping the header {}", concatColumns);
        } else {

            AvocadoSale sale = new AvocadoSale();
            sale.setId(Long.valueOf(fields[0].trim()));
            try {
                sale.setDate(new SimpleDateFormat("yyyy-MM-dd").parse(fields[1].trim()));
            } catch (ParseException e) {
                LOG.error("Error occurred while parsing date {}", fields[1].trim(), e);
            }
            sale.setAveragePrice(new BigDecimal(fields[2].trim()));
            sale.setTotalVolume(Double.valueOf(fields[3].trim()));

            sale.setVolumeWithPLU4046(Double.valueOf(fields[4].trim()));
            sale.setVolumeWithPLU4225(Double.valueOf(fields[5].trim()));
            sale.setVolumeWithPLU4770(Double.valueOf(fields[6].trim()));
            sale.setTotalBags(Double.valueOf(fields[7].trim()));
            sale.setSmallBags(Double.valueOf(fields[8].trim()));
            sale.setLargeBags(Double.valueOf(fields[9].trim()));
            sale.setxLargeBags(Double.valueOf(fields[10].trim()));
            sale.setType(fields[11].trim());
            sale.setYear(Long.valueOf(fields[12].trim()));
            sale.setRegion(fields[13].trim());
            outputReceiver.output(sale);
        }
    }
}
