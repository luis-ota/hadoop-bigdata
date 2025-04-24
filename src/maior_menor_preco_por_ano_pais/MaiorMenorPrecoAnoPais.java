package maior_menor_preco_por_ano_pais;

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

public class MaiorMenorPrecoAnoPais {

    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();

        Configuration c = new Configuration();

        // arquivo de entrada
        Path input = new Path("in/operacoes_comerciais_inteira.csv");

        // arquivo de saida
        Path output = new Path("src/maior_menor_preco_por_ano_pais/maior_menor_preco_por_ano_pais");

        // apagar o diretorio de output se existir
        FileSystem fs = FileSystem.get(c);
        if (fs.exists(output)) {
            fs.delete(output, true);
        }

        // criacao do job e seu nome
        Job j = new Job(c, "MaiorMenorPrecoAnoPais");

        // registro das classes
        j.setJarByClass(MaiorMenorPrecoAnoPais.class);
        j.setMapperClass(MaiorMenorPrecoAnoPaisMapper.class);
        j.setReducerClass(MaiorMenorPrecoAnoPaisReducer.class);

        // definicao dos tipos de saida
        j.setMapOutputKeyClass(AnoPaisWritable.class);
        j.setMapOutputValueClass(DoubleWritable.class);

        j.setOutputKeyClass(AnoPaisWritable.class);
        j.setOutputValueClass(MaxMinWritable.class);

        // cadastro dos arquivos de entrada e saida
        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);

        // lanca o job e aguarda sua execucao
        System.exit(j.waitForCompletion(true) ? 0 : 1);
    }

    public static class MaiorMenorPrecoAnoPaisMapper
            extends Mapper<LongWritable, Text, AnoPaisWritable, DoubleWritable> {

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {



            String[] partes = value.toString().split(";");
            if (partes.length < 9 || partes[8].isEmpty() || partes[0].equals("country_or_area")) return;

            try {
                double amount = Double.parseDouble(partes[8]);
                context.write(
                        new AnoPaisWritable(partes[1], partes[0]),
                        new DoubleWritable(amount)
                );
            } catch (NumberFormatException e) {
                context.getCounter("Erros", "AmountInvalido").increment(1);
            }
        }
    }

    public static class MaiorMenorPrecoAnoPaisReducer
            extends Reducer<AnoPaisWritable, DoubleWritable, AnoPaisWritable, MaxMinWritable> {

        @Override
        protected void reduce(AnoPaisWritable chave,
                              Iterable<DoubleWritable> valores,
                              Context context)
                throws IOException, InterruptedException {

            double max = Double.NEGATIVE_INFINITY;
            double min = Double.POSITIVE_INFINITY;

            for (DoubleWritable val : valores) {
                double v = val.get();
                if (v <= 0) continue;  // opcional: descarta zeros
                if (v > max) max = v;
                if (v < min) min = v;
            }

            if (min == Double.POSITIVE_INFINITY) {
                // nenhum valor válido > 0; nada a emitir
                return;
            }

            // aqui não concatenamos nada: usamos o Writable direto
            context.write(chave, new MaxMinWritable(max, min));
        }
    }

}