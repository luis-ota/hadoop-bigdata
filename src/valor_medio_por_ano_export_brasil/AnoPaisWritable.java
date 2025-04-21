package valor_medio_por_ano_export_brasil;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class AnoPaisWritable implements WritableComparable<AnoPaisWritable> {

    private String ano;
    private String pais;

    public AnoPaisWritable() { }

    public AnoPaisWritable(String ano, String pais) {
        this.ano  = ano;
        this.pais = pais;
    }

    public String getAno()  { return ano; }
    public String getPais() { return pais; }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(ano);
        out.writeUTF(pais);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        ano  = in.readUTF();
        pais = in.readUTF();
    }

    @Override
    public int compareTo(AnoPaisWritable o) {
        int cmp = ano.compareTo(o.ano);
        return (cmp != 0) ? cmp : pais.compareTo(o.pais);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof AnoPaisWritable)) return false;
        AnoPaisWritable that = (AnoPaisWritable) o;
        return ano.equals(that.ano) && pais.equals(that.pais);
    }

    @Override
    public int hashCode() {
        return Objects.hash(ano, pais);
    }

    @Override
    public String toString() {
        return ano + "|" + pais;
    }
}
