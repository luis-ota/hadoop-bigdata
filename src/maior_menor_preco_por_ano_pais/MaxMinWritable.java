package maior_menor_preco_por_ano_pais;

import org.apache.hadoop.io.Writable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.DecimalFormat;

public class MaxMinWritable implements Writable {

    private double max;
    private double min;

    public MaxMinWritable() { }

    public MaxMinWritable(double max, double min) {
        this.max = max;
        this.min = min;
    }

    public double getMax() { return max; }
    public double getMin() { return min; }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeDouble(max);
        out.writeDouble(min);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        max = in.readDouble();
        min = in.readDouble();
    }

    @Override
    public String toString() {
        // aqui usamos apenas para formatar a saída de texto, não para lógica de chave/valor
        return new DecimalFormat("0.####################").format(max) + "\t" + min;
    }
}
