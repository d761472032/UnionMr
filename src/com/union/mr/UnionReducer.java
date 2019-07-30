package com.union.mr;

import com.union.util.Contants;
import com.union.util.LineParse;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashSet;

public class UnionReducer extends Reducer<Text, Text, Text, Text> {

//    private Log log = LogFactory.getLog(UnionReducer.class);

    private LineParse lineParse = new LineParse();

    private StringBuilder stringBuilder = new StringBuilder();

    private HashSet<String> matchValues = new HashSet<>();

    private Text valueText = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
//        for (Text text : values)
//            context.write(key, text);
        lineParse.parse(key.toString());
        if (lineParse.getValueByIndex(Contants.NUM_ONE).equals(Contants.STR_ZERO)) {
            // a\t0, 1
            stringBuilder.setLength(0);
            matchValues.clear();

            stringBuilder.append(lineParse.getValueByIndex(Contants.NUM_ZERO));
            for (Text text : values)
                matchValues.add(text.toString());
        } else {
            if (stringBuilder.toString().equals(lineParse.getValueByIndex(Contants.NUM_ZERO)))
                for (String t : matchValues) {
                    valueText.set(t);
                    for (Text value : values)
                        context.write(valueText, value);
                    valueText.clear();
                }
        }
    }

}
