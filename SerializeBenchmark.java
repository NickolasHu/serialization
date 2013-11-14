package serialization;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Scanner;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorOutputStream;
import org.xerial.snappy.Snappy;

import com.alibaba.fastjson.JSON;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

public class SerializeBenchmark {
	
	public byte[] serializeJavaUtil(Serializable o) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutputStream    os  = new ObjectOutputStream(bos);
        
        os.writeObject(o);
        os.close();
        bos.close();
        return bos.toByteArray();
	}
	
	public Object deserializeJavaUtil(byte[] input) throws IOException, ClassNotFoundException {
		ByteArrayInputStream bis = new ByteArrayInputStream(input);
		ObjectInputStream 	 is  = new ObjectInputStream(bis);
		
		Object out = is.readObject();
		is.close();
		bis.close();
		return out;
	}
	
	public byte[] serializeKryo(Serializable o) {
		Kryo kryo = new Kryo();
		ByteArrayOutputStream byteArrStream = new ByteArrayOutputStream();
		Output output = new Output(byteArrStream);
		
		kryo.writeObject(output);
		
		byte[] out = output.toBytes();
		output.close();
		return out;
	}
	
	public Object deserializeKryo(byte[] in) {
		Kryo kryo = new Kryo();
		ByteArrayInputStream byteArrStream = new ByteArrayInputStream(in);
		Input input = new Input(byteArrStream);
		
		Object o = kryo.readObject(input, Object.class);
		
		input.close();
		return o;
	}
	
	public byte[] compressSnappy(byte[] in) throws IOException{
		return Snappy.compress(in);
	}
	
	public byte[] uncompressSnappy(byte[] in) throws IOException {
		return Snappy.uncompress(in);
	}
	
    public byte[] compressGZip(byte[] in) {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        GZIPOutputStream      gz  = null;

        try {
            gz = new GZIPOutputStream(bos);
            gz.write(in);
        } catch (IOException e) {
            throw new RuntimeException("IO exception compressing data", e);
        } finally {
            try {
                gz.close();
                bos.close();
            } catch (Exception e) {
                // should not happen
            }
        }
 
        return bos.toByteArray();
    }
    
    public byte[] uncompressGzip(byte[] in) {
		ByteArrayOutputStream bos = null;

		if (in != null) {
			ByteArrayInputStream bis = new ByteArrayInputStream(in);
			bos = new ByteArrayOutputStream();
			GZIPInputStream gis = null;

			try {
				gis = new GZIPInputStream(bis);

				byte[] buf = new byte[8192];
				int r = -1;

				while ((r = gis.read(buf)) > 0) {
					bos.write(buf, 0, r);
				}
			} catch (IOException e) {
				throw new RuntimeException(e);
			} finally {
				try {
					gis.close();
					bis.close();
					bos.close();
				} catch (Exception e) {
					// should not happen
				}
			}
		}

		return (bos == null) ? null : bos.toByteArray();
    }
    
    public byte[] compressBZip2(byte[] in) {
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		BZip2CompressorOutputStream bz2os = null;

		try {
			bz2os = new BZip2CompressorOutputStream(bos);
			bz2os.write(in);
		} catch (IOException e) {
			throw new RuntimeException("IO exception compressing data", e);
		} finally {
			try {
				bz2os.close();
				bos.close();
			} catch (Exception e) {
				// should not happen
			}
		}
		
		return bos.toByteArray();
    }
    
	public byte[] uncompressBZip2(byte[] in) {
		ByteArrayOutputStream bos = null;

		if (in != null) {
			ByteArrayInputStream bis = new ByteArrayInputStream(in);
			bos = new ByteArrayOutputStream();
			BZip2CompressorInputStream bz2is = null;

			try {
				bz2is = new BZip2CompressorInputStream(bis);
				byte[] buf = new byte[8192];
				int r = -1;

				while ((r = bz2is.read(buf)) > 0) {
					bos.write(buf, 0, r);
				}
			} catch (IOException e) {
				throw new RuntimeException(e);
			} finally {
				try {
					bz2is.close();
					bis.close();
					bos.close();
				} catch (Exception e) {
					// should not happen
				}
			}
		}

		return (bos == null) ? null : bos.toByteArray();
	}
	
	
	public static final void main(String args[]) throws IOException{
		String content = new Scanner(new File("D:/tmp/result.json")).useDelimiter("\\Z").next();
		Serializable obj = (Serializable) JSON.parse(content);
		
		SerializeBenchmark benchmark = new SerializeBenchmark();
		int TIMES = 1;
		
		// kryo serialization
		long start = System.nanoTime();
		byte[] serializedArr = null;
		for (int i=0; i< TIMES; ++i) {
			serializedArr = benchmark.serializeKryo(obj);
		}
		double dt = (double)(System.nanoTime() - start) / (TIMES * 1000 * 1000);
		System.out.println("serialize Kryo: avg time-"+dt+"ms size-"+serializedArr.length);
		
		// java util serialization
		start = System.nanoTime();
		
		for (int i=0; i< TIMES; ++i) {
			serializedArr = benchmark.serializeJavaUtil(obj);
		}
		dt = (double)(System.nanoTime() - start) / (TIMES * 1000 * 1000);
		System.out.println("serialize JavaUtil: avg time-"+dt+"ms size-"+serializedArr.length);
		
		// gzip compression
		start = System.nanoTime();
		byte[] compressedArr = null;
		for (int i=0; i< TIMES; ++i) {
			compressedArr = benchmark.compressGZip(serializedArr);
		}
		dt = (double)(System.nanoTime() - start) / TIMES;
		double compressRatio = (double) serializedArr.length / compressedArr.length;
		System.out.println("compress GZip: avg time-"+dt+" compress ratio-"+compressRatio);
		
		// snappy compression
		start = System.nanoTime();
		for (int i=0; i< TIMES; ++i) {
			compressedArr = benchmark.compressSnappy(serializedArr);
		}
		dt = (double)(System.nanoTime() - start) / TIMES;
		compressRatio = (double) serializedArr.length / compressedArr.length;
		System.out.println("compress Snappy: avg time-"+dt+"ns compress ratio-"+compressRatio);
		
		// bzip2 compression
		start = System.nanoTime();
		for (int i=0; i< TIMES; ++i) {
			compressedArr = benchmark.compressBZip2(serializedArr);
		}
		dt = (double)(System.nanoTime() - start) / TIMES;
		compressRatio = (double) serializedArr.length / compressedArr.length;
		System.out.println("compress BZip2: avg time-"+dt+"ns compress ratio-"+compressRatio);
		
		
		Object object = null;
		byte[] byteArr = null;
		start = System.nanoTime();
		for (int i=0; i<TIMES; ++i) {
			byteArr = benchmark.compressGZip(benchmark.serializeKryo(obj));
			object = benchmark.deserializeKryo(benchmark.uncompressGzip(byteArr));
		}
		dt = (double)(System.nanoTime() - start) / TIMES;
		System.out.println("serialize kryo compress gzip: avg time-"+dt+"ns size-"+byteArr.length);
		
		start = System.nanoTime();
		for (int i=0; i<TIMES; ++i) {
			byteArr = benchmark.compressSnappy(benchmark.serializeKryo(obj));
			object = benchmark.deserializeKryo(benchmark.uncompressSnappy(byteArr));
		}
		dt = (double)(System.nanoTime() - start) / TIMES;
		System.out.println("serialize kryo compress snappy: avg time-"+dt+"ns size-"+byteArr.length);
	
		start = System.nanoTime();
		for (int i=0; i<TIMES; ++i) {
			byteArr = benchmark.compressBZip2(benchmark.serializeKryo(obj));
			object = benchmark.deserializeKryo(benchmark.uncompressBZip2(byteArr));
		}
		dt = (double)(System.nanoTime() - start) / TIMES;
		System.out.println("serialize kryo compress bzip2: avg time-"+dt+"ns size-"+byteArr.length);
	}
}
