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
import com.alibaba.fastjson.JSONObject;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

public class SerializeBenchmark {
	
	Kryo kryo = new Kryo();
	
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
	
	public byte[] serializeKryo(Object o) {
//		kryo.register(JSONObject.class, new FieldSerializer(kryo, JSONObject.class));
		ByteArrayOutputStream byteArrStream = new ByteArrayOutputStream();
		Output output = new Output(byteArrStream);
		
		kryo.writeObject(output, o);
		
		output.close();
		return byteArrStream.toByteArray();
	}
	
	public Object deserializeKryo(byte[] in) {
		ByteArrayInputStream byteArrStream = new ByteArrayInputStream(in);
		Input input = new Input(byteArrStream);
		
		Object o =  kryo.readObject(input, Object.class);
		
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
	
	
	public static final void main(String args[]) throws IOException, ClassNotFoundException{
		String content = new Scanner(new File("D:/tmp/result.json")).useDelimiter("\\Z").next();
		JSONObject obj = (JSONObject) JSON.parse(content);
		
		SerializeBenchmark benchmark = new SerializeBenchmark();
		int TIMES = 1000;
		
		testSerialize(benchmark, TIMES, obj);
//		testCompress(benchmark, TIMES, obj);
	}

	private static void testSerialize(SerializeBenchmark benchmark, int TIMES,
			JSONObject obj) throws IOException, ClassNotFoundException {

		byte[] byteArr = null;
		long start = System.nanoTime();
		@SuppressWarnings("unused")
		Object dumpObj = null;
		
		for (int i=0; i<TIMES; ++i) {
			byteArr = benchmark.serializeJavaUtil(obj);
			dumpObj = benchmark.deserializeJavaUtil(byteArr);
		}
		double dt = (double)(System.nanoTime() - start) / TIMES;
		System.out.println("serialize and deserialize with java.util : avg time-"+dt+"ns size-"+byteArr.length);
		
		start = System.nanoTime();
		for (int i=0; i<TIMES; ++i) {
			byteArr = benchmark.serializeKryo(obj);
			dumpObj = benchmark.deserializeKryo(byteArr);
//			JSONObject dumpJSObj = (JSONObject)dumpObj;
		}
		dt = (double)(System.nanoTime() - start) / TIMES;
		System.out.println("serialize and deserialize with kryo: avg time-"+dt+"ns size-"+byteArr.length);
	}

	private static void testCompress(SerializeBenchmark benchmark, int TIMES, Serializable obj) throws IOException, ClassNotFoundException {
		byte[] byteArr = null;
		long start = System.nanoTime();
		byte[] initArr = benchmark.serializeJavaUtil(obj);
		@SuppressWarnings("unused")
		byte[] dumpArr = null;
		System.out.println("init size "+initArr.length);
		
		for (int i=0; i<TIMES; ++i) {
			byteArr = benchmark.compressGZip(initArr);
			dumpArr = benchmark.uncompressGzip(byteArr);
		}
		double dt = (double)(System.nanoTime() - start) / TIMES;
		System.out.println("serialize java.util compress gzip: avg time-"+dt+"ns size-"+byteArr.length);
		
		start = System.nanoTime();
		for (int i=0; i<TIMES; ++i) {
			byteArr = benchmark.compressSnappy(initArr);
			dumpArr = benchmark.uncompressSnappy(byteArr);
		}
		dt = (double)(System.nanoTime() - start) / TIMES;
		System.out.println("serialize java.util compress snappy: avg time-"+dt+"ns size-"+byteArr.length);
	
		start = System.nanoTime();
		for (int i=0; i<TIMES; ++i) {
			byteArr = benchmark.compressBZip2(initArr);
			dumpArr = benchmark.uncompressBZip2(byteArr);
		}
		dt = (double)(System.nanoTime() - start) / TIMES;
		System.out.println("serialize java.util compress bzip2: avg time-"+dt+"ns size-"+byteArr.length);
	}
}
