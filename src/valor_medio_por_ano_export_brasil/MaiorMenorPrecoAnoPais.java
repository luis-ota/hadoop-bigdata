package valor_medio_por_ano_export_brasil;

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
import trasnsacoes_por_ano.TransacoesPorAno;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.DecimalFormat;

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
        j.setMapperClass(BrazilTransactionMapper.class);
        j.setReducerClass(BrazilTransactionReducer.class);

        // definicao dos tipos de saida
        j.setMapOutputKeyClass(AnoPaisWritable.class);
        j.setMapOutputValueClass(DoubleWritable.class);
        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(Text.class);

        // cadastro dos arquivos de entrada e saida
        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);

        // lanca o job e aguarda sua execucao
        System.exit(j.waitForCompletion(true) ? 0 : 1);
    }

    public static class BrazilTransactionMapper
            extends Mapper<LongWritable, Text, AnoPaisWritable, DoubleWritable> {

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            if (key.get() == 0) return;  // pula header

            String[] partes = value.toString().split(";");
            if (partes.length < 9 || partes[8].isEmpty()) return;

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

    public static class BrazilTransactionReducer
            extends Reducer<AnoPaisWritable, DoubleWritable, Text, Text> {

        private Text chaveFinal = new Text();
        private Text resultado = new Text();

        @Override
        protected void reduce(AnoPaisWritable chave, Iterable<DoubleWritable> valores, Context context)
                throws IOException, InterruptedException {

            double max = Double.MIN_VALUE;
            double min = Double.MAX_VALUE;

            // Calcular min e max
            for (DoubleWritable val : valores) {
                double atual = val.get();
                if (atual > max) max = atual;
                if (atual < min) min = atual;
            }

            // Formatar saída
            chaveFinal.set(chave.getAno() + "|" + chave.getPais());
            resultado.set("Maior: " + max + " | Menor: " + min);

            context.write(chaveFinal, resultado);
        }
    }
}