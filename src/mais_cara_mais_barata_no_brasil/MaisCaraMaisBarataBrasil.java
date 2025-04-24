package mais_cara_mais_barata_no_brasil;
import java.text.DecimalFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.BasicConfigurator;

import java.io.IOException;

public class MaisCaraMaisBarataBrasil {

    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();

        Configuration c = new Configuration();

        // arquivo de entrada
        Path input = new Path("in/operacoes_comerciais_inteira.csv");

        // arquivo de saida
        Path output = new Path("src/mais_cara_mais_barata_no_brasil/mais_cara_mais_barata_no_brasil_resultados");

        // apagar o diretorio de output se existir
        FileSystem fs = FileSystem.get(c);
        if (fs.exists(output)) {
            fs.delete(output, true);
        }

        // criacao do job e seu nome
        Job j = new Job(c, "MaisCaraMaisBarataBrasil");

        // registro das classes
        j.setJarByClass(MaisCaraMaisBarataBrasil.class);
        j.setMapperClass(MaisCaraMaisBarataBrasilMapper.class);
        j.setReducerClass(MaisCaraMaisBarataBrasilReducer.class);

        // definicao dos tipos de saida
        j.setMapOutputKeyClass(Text.class);
        j.setMapOutputValueClass(DoubleWritable.class);
        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(Text.class);

        // cadastro dos arquivos de entrada e saida
        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);

        // lanca o job e aguarda sua execucao
        System.exit(j.waitForCompletion(true) ? 0 : 1);
    }

    public static class MaisCaraMaisBarataBrasilMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String[] parts = value.toString().split(";");

            if (parts.length < 10 ||
                    !parts[0].equals("Brazil") ||
                    !parts[1].equals("2016") ||
                    parts[5].isEmpty() ||
                    parts[1].equals("year")) {
                return;
            }

            try {
                double price = Double.parseDouble(parts[5]);
                context.write(new Text("Brazil_2016"), new DoubleWritable(price));
            } catch (NumberFormatException e) {
                // tratar numeros invalidos
            }
        }
    }

    public static class MaisCaraMaisBarataBrasilReducer extends Reducer<Text, DoubleWritable, Text, Text> {
        public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
                throws IOException, InterruptedException {

            double max = Double.MIN_VALUE;
            double min = Double.MAX_VALUE;

            for (DoubleWritable val : values) {
                double current = val.get();
                if (current > max) max = current;
                if (current < min) min = current;
            }

            context.write(new Text("mais cara 2016"), new Text(new DecimalFormat("#.##########").format(max)));
            context.write(new Text("mais barata 2016"), new Text(new DecimalFormat("#.##########").format(min)));
        }
    }
}