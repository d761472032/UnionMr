package com.union.mr;

import com.union.util.Contants;
import com.union.util.LineParse;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class UnionMapper extends Mapper<LongWritable, Text, Text, Text> {

//    private Log log = LogFactory.getLog(UnionMapper.class);

    private StringBuilder stringBuilder = new StringBuilder();

    private Text keyText = new Text();

    private Text valueText = new Text();

    private LineParse lineParse = new LineParse();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        lineParse.parse(value.toString());

        if (Contants.NUM_TWO == lineParse.getCount()) {
            stringBuilder.append(lineParse.getValueByIndex(Contants.NUM_ONE));
            stringBuilder.append(Contants.split);
            stringBuilder.append(Contants.NUM_ZERO);
            keyText.set(stringBuilder.toString());
            stringBuilder.setLength(0);

            stringBuilder.append(lineParse.getValueByIndex(Contants.NUM_ZERO));
            valueText.set(stringBuilder.toString());

//            log.info(keyText.toString() + "_" + value.toString());
            context.write(keyText, valueText);
            valueText.clear();
        } else {
            stringBuilder.append(lineParse.getValueByIndex(Contants.NUM_ZERO));
            stringBuilder.append(Contants.split);
            stringBuilder.append(Contants.NUM_ONE);
            keyText.set(stringBuilder.toString());

//            log.info(keyText.toString() + "_" + value.toString());
            context.write(keyText, value);
        }

        stringBuilder.setLength(0);
        keyText.clear();
    }

}
