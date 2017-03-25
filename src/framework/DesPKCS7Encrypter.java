package framework;


import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.security.spec.AlgorithmParameterSpec;

import javax.crypto.Cipher;
import javax.crypto.CipherInputStream;
import javax.crypto.CipherOutputStream;
import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.DESKeySpec;
import javax.crypto.spec.IvParameterSpec;

import sun.misc.BASE64Decoder;

public final class DesPKCS7Encrypter
{
  Cipher ecipher;
  Cipher dcipher;

  DesPKCS7Encrypter(byte[] keyBytes, byte[] ivBytes)
    throws Exception
  {
    Init(keyBytes, ivBytes);
  }

  DesPKCS7Encrypter(DESKeySpec keySpec, IvParameterSpec ivSpec) throws Exception
  {
    Init(keySpec, ivSpec);
  }

  private void Init(byte[] keyBytes, byte[] ivBytes) throws Exception
  {
    DESKeySpec dks = new DESKeySpec(keyBytes);
    SecretKeyFactory keyFactory = SecretKeyFactory.getInstance("DES");
    SecretKey key = keyFactory.generateSecret(dks);
    IvParameterSpec iv = new IvParameterSpec(ivBytes);
    Init(key, iv);
  }

  private void Init(DESKeySpec keySpec, IvParameterSpec iv)
    throws Exception
  {
    SecretKeyFactory keyFactory = SecretKeyFactory.getInstance("DES");
    SecretKey key = keyFactory.generateSecret(keySpec);
    Init(key, iv);
  }

  private void Init(SecretKey key, IvParameterSpec iv) throws Exception
  {
    AlgorithmParameterSpec paramSpec = iv;
    try
    {
      this.ecipher = Cipher.getInstance("DES/CBC/NoPadding");
      this.dcipher = Cipher.getInstance("DES/CBC/NoPadding");

      this.ecipher.init(1, key, paramSpec);
      this.dcipher.init(2, key, paramSpec);
    }
    catch (Exception e)
    {
      throw e;
    }
  }

  public void encrypt(InputStream in, OutputStream out) throws Exception {
    try {
      out = new CipherOutputStream(out, this.ecipher);
      byte[] buf = new byte[this.ecipher.getBlockSize()];

      int numRead = 0;
      boolean bBreak;
      do {
        numRead = in.read(buf);
        bBreak = false;
        if ((numRead == -1) || (numRead < buf.length))
        {
          int pos = numRead == -1 ? 0 : numRead;
          byte byteFill = (byte)(buf.length - pos);
          for (int i = pos; i < buf.length; i++)
          {
            buf[i] = byteFill;
          }
          bBreak = true;
        }
        out.write(buf);
      }
      while (!bBreak);

      out.close();
    }
    catch (Exception e)
    {
      throw e;
    }
  }
  

  private static final BASE64Decoder BASE64_DECODER = new BASE64Decoder();
  private static final byte[] IV = { 18, 52, 86, 120, -112, -85, -51, -17 };
  public String ecrypt_111(String input, String ecryptKey)
  {
 
    try
    {
      byte[] byteKey = ecryptKey.getBytes("ASCII");

      DesPKCS7Encrypter encrypter = new DesPKCS7Encrypter(byteKey, IV);

      ByteArrayInputStream bais = new ByteArrayInputStream(BASE64_DECODER.decodeBuffer(input));

      ByteArrayOutputStream baos = new ByteArrayOutputStream();

      encrypter.encrypt(bais,baos);
//      encrypter.decrypt(bais, baos);

      String result = baos.toString("utf-8");
      return result;
    }
    catch (Exception e)
    {
    }return null;
  }

  public void decrypt(InputStream in, OutputStream out)
    throws Exception
  {
    try
    {
      in = new CipherInputStream(in, this.dcipher);
      byte[] buf = new byte[this.dcipher.getBlockSize()];

      int numRead = 0;
      while ((numRead = in.read(buf)) >= 0)
      {
        if (in.available() > 0)
        {
          out.write(buf, 0, numRead);
        }
        else
        {
          byte byteBlock = buf[(buf.length - 1)];
          int i = 0;
          for (i = buf.length - byteBlock; (i >= 0) && (i < buf.length); i++)
          {
            if (buf[i] != byteBlock)
            {
              break;
            }
          }

          if (i == buf.length)
          {
            out.write(buf, 0, buf.length - byteBlock);
          }
          else
          {
            out.write(buf);
          }
        }
      }

      out.close();
    }
    catch (Exception e)
    {
      throw e;
    }
  }
}
