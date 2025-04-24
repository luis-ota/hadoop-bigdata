package valor_medio_por_ano_brasil;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.BasicConfigurator;

import java.io.IOException;
import java.text.DecimalFormat;

public class ValorMedioPorAnoBrasil {

    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();

        Configuration c = new Configuration();

        // arquivo de entrada
        Path input = new Path("in/operacoes_comerciais_inteira.csv");

        // arquivo de saida
        Path output = new Path("src/valor_medio_por_ano_brasil/valor_medio_por_ano_brasil_resultados");

        // apagar o diretorio de output se existir
        FileSystem fs = FileSystem.get(c);
        if (fs.exists(output)) {
            fs.delete(output, true);
        }

        // criacao do job e seu nome
        Job j = new Job(c, "ValorMedioPorAnoBrasil");

        // registro das classes
        j.setJarByClass(ValorMedioPorAnoBrasil.class);
        j.setMapperClass(ValorMedioPorAnoBrasilMapper.class);
        j.setReducerClass(ValorMedioPorAnoBrasilReducer.class);

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

    public static class ValorMedioPorAnoBrasilMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] parts = value.toString().split(";");
            if (parts.length < 10 || !parts[0].equals("Brazil") || parts[5].isEmpty() || parts[1].isEmpty()){return;}

            context.write(new Text(parts[1]), new DoubleWritable(new Double(parts[5])));
        }
    }

    public static class ValorMedioPorAnoBrasilReducer extends Reducer<Text, DoubleWritable, Text, Text> {
        public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
                throws IOException, InterruptedException {

            double sum = 0;
            int count = 0;

            for (DoubleWritable val : values) {
                sum += val.get();
                count++;
            }

            if (count > 0) {
                double media = sum / count;
                context.write(key, new Text(String.valueOf(new DecimalFormat("0.####################").format(media))));
            }

        }
    }
}