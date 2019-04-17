package dc.common.hbase;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.MD5Hash;

import java.util.Random;
import java.util.UUID;

public class HashRowKeyGenerator {

    private long currentId = 1;
    private long currentTime = System.currentTimeMillis();
    private Random random = new Random();
    public byte[] nextId() {
        try {
            currentTime += random.nextInt(1000);
            byte[] lowT = Bytes.copy(Bytes.toBytes(currentTime), 4, 4);
            byte[] lowU = Bytes.copy(Bytes.toBytes(currentId), 4, 4);
            return Bytes.add(MD5Hash.getMD5AsHex(Bytes.add(lowU, lowT)).substring(0, 8).getBytes(),
                    Bytes.toBytes(currentId));
        } finally {
            currentId++;
        }
    }

    public byte[] nextId2() {

            currentTime += random.nextInt(1000);

            return (MD5Hash.getMD5AsHex(("/hollysys/"+ UUID.randomUUID()).getBytes())+"_"+currentTime).getBytes();

    }

}
