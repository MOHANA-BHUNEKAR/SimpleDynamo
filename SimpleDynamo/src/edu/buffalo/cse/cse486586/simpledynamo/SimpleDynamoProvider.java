package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Formatter;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import android.content.ContentProvider;
import android.content.ContentResolver;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.database.sqlite.SQLiteDatabase;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

public class SimpleDynamoProvider extends ContentProvider {

	public static final Uri CONTENT_URI = Uri.parse("content://edu.buffalo.cse.cse486586.simpledynamo.provider/"+ DBCreation.TABLE_NAME);
		String port_avd,portStr,sucport_avd;
	 int poolSize =60;
     int maxPool = 80;
     int kAT = 10;
     public SQLiteDatabase db;
	ArrayList<String> listport=new ArrayList<String>();
	ArrayList<String> porthash=new ArrayList<String>();
	 Executor threadpoolExecutor;
	static final String TAG = SimpleDynamoProvider.class.getSimpleName();
	 String data;
	    boolean isDataNotReceived=true;
	int count=0;

	public static final int SERVER_PORT = 10000;
	
		@Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {
		// TODO Auto-generated method stub

		db.delete("key_value_pair"," key!= "+portStr , null);
		
		return 0;
	}

	@Override
	public String getType(Uri uri) {
		// TODO Auto-generated method stub
		return null;
	}
	private Uri buildUri(String scheme, String authority) {
        // TODO Auto-generated method stub

        Uri.Builder uriBuilder = new Uri.Builder();
        uriBuilder.authority(authority);
        uriBuilder.scheme(scheme);
        return uriBuilder.build();
        // return null;
    }

	@Override
	public Uri insert(Uri uri, ContentValues values) {
		// TODO Auto-generated method stub
		
		Log.v(TAG,"In insert section");
		
		 String key = (String) values.get(DBCreation.COLUMN_KEY);
         String value = (String) values.get(DBCreation.COLUMN_VAL);
         String regkeva = key+"^^"+value;
         Log.v(TAG,"-------------In line 77 with regkeyva: sending portStr: "+portStr+" with reg^^key"+regkeva);
        
         String keyhash1=null;
         String idenavd=null;
         String sucavd=null;
         String sucsucavd=null;
         String portnum1=null;
         String sucportnum1=null;
         String sucsucportnum1=null;
         int sizequ=porthash.size();
			try {
				keyhash1 = genHash(key);
			} catch (NoSuchAlgorithmException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
         
			 if (keyhash1.compareTo(porthash.get(0)) <= 0|| keyhash1.compareTo(porthash.get(sizequ - 1)) > 0) {
				 
				 Log.v(TAG,"-----------In line 111--------");
                 idenavd = listport.get(0);
                 sucavd=listport.get(1);
                 sucsucavd=listport.get(2); 
                 Log.v(TAG,"5562 insert and it's successors are: "+idenavd+" "+sucavd+"  "+sucsucavd);
                 portnum1 = String.valueOf((Integer.parseInt(idenavd) * 2));
                 sucportnum1 = String.valueOf((Integer.parseInt(sucavd) * 2));  
                 sucsucportnum1 = String.valueOf((Integer.parseInt(sucsucavd) * 2));             
                 new ClientTask().executeOnExecutor(threadpoolExecutor,regkeva, portnum1);                 
                 new ClientTask().executeOnExecutor(threadpoolExecutor,regkeva, sucportnum1);                 
                 new ClientTask().executeOnExecutor(threadpoolExecutor,regkeva, sucsucportnum1);
             }

             else {

                 for (int y = 1; y < porthash.size(); y++) {

                     if (keyhash1.compareTo(porthash.get(y - 1)) > 0 && keyhash1.compareTo(porthash.get(y)) <= 0) {
                        // idenavd = listport.get(y);
                         if((y!=porthash.size()-2) && (y!=porthash.size()-1))
                         {
                        	
                        	 Log.v(TAG,"In Line 162 with y value: "+y);
                             idenavd = listport.get(y);
                        	  sucavd=listport.get(y+1);
                              sucsucavd=listport.get(y+2);	 
                         portnum1 = String.valueOf((Integer.parseInt(idenavd)*2));
                         sucportnum1 = String.valueOf((Integer.parseInt(sucavd) * 2));
                         sucsucportnum1 = String.valueOf((Integer.parseInt(sucsucavd) * 2));
                         new ClientTask().executeOnExecutor(threadpoolExecutor,regkeva,portnum1);
                         new ClientTask().executeOnExecutor(threadpoolExecutor,regkeva,sucportnum1);
                         new ClientTask().executeOnExecutor(threadpoolExecutor,regkeva,sucsucportnum1);
                         break;
                     }
                         
                         else if(y==porthash.size()-2)
                             {
                        	 
                        	 Log.v(TAG,"iN LINE 194 with y value: "+y);
                             idenavd = listport.get(y);
                            	 sucavd=listport.get(y+1);
                                 sucsucavd=listport.get(0);	 
                            portnum1 = String.valueOf((Integer.parseInt(idenavd)*2));
                            sucportnum1 = String.valueOf((Integer.parseInt(sucavd) * 2));
                            sucsucportnum1 = String.valueOf((Integer.parseInt(sucsucavd) * 2));                  
                            new ClientTask().executeOnExecutor(threadpoolExecutor,regkeva,portnum1);
                            new ClientTask().executeOnExecutor(threadpoolExecutor,regkeva,sucportnum1);
                            new ClientTask().executeOnExecutor(threadpoolExecutor,regkeva,sucsucportnum1);
                            break;
                          }
                         
                         
                         else
                         {
                        	 if(y==porthash.size()-1)
                        	 {
                        		Log.v(TAG,"In line 228 with y value: "+y); 
                                idenavd = listport.get(y);
                        		 sucavd=listport.get(0);
                                 sucsucavd=listport.get(1);	 
                            portnum1 = String.valueOf((Integer.parseInt(idenavd)*2));
                            sucportnum1 = String.valueOf((Integer.parseInt(sucavd) * 2));
                            sucsucportnum1 = String.valueOf((Integer.parseInt(sucsucavd) * 2));                        		 
                            new ClientTask().executeOnExecutor(threadpoolExecutor,regkeva,portnum1);
                            new ClientTask().executeOnExecutor(threadpoolExecutor,regkeva,sucportnum1);
                            new ClientTask().executeOnExecutor(threadpoolExecutor,regkeva,sucsucportnum1);
                            break;
                        	 }
                        	 
                        	 
                        	 
                        	 
                        	 
                         }
                 }
                 //return null;
             }

         }
        
		return null;
	}

	@Override
	public boolean onCreate() {
		// TODO Auto-generated method stub
		
		Log.v(TAG,"In line 197");

		DBCreation dn = new DBCreation(getContext());
		db=dn.getWritableDatabase();
		TelephonyManager tel = (TelephonyManager) getContext()
				.getSystemService(Context.TELEPHONY_SERVICE);
		portStr = tel.getLine1Number().substring(
				tel.getLine1Number().length() - 4);
		port_avd = String.valueOf((Integer.parseInt(portStr) * 2));
		//sucport_avd= String.valueOf(((Integer.parseInt(portStr)+2) * 2));
		Log.v(TAG,"SUCCESOR FOR PORT: "+port_avd+"  is: "+sucport_avd);
		Log.v(TAG, "portStr: " + portStr);
		String myhashedvalue=null;
		
		try {
			myhashedvalue = genHash(portStr);
		} catch (NoSuchAlgorithmException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
		
		
		listport.add("5562");
		listport.add("5556");
		listport.add("5554");
		listport.add("5558");
		listport.add("5560");
		int sizequ=listport.size();
		for(int i=0;i<listport.size();i++)
		{
			String hashedvalue=null;
			try {
				hashedvalue = genHash(listport.get(i));
			} catch (NoSuchAlgorithmException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			porthash.add(hashedvalue);
			
		}
		


		try {
            BlockingQueue<Runnable> queue = new LinkedBlockingQueue<Runnable>(maxPool);
            threadpoolExecutor = new ThreadPoolExecutor(poolSize, maxPool, kAT, TimeUnit.SECONDS, queue);
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
           Log.v(TAG,"Server Socket created: and call new ServerTask().execute........");
           
           
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
           
           
        } catch (IOException e) {
            
            Log.e(TAG, "Can't create a ServerSocket");
            return false;
        }
				
		Log.v(TAG,"-----In line 249 on create()-----");
		
		String str = "select * from "+ DBCreation.TABLE_NAME + " where "+ DBCreation.COLUMN_KEY + "='"+ portStr + "'";
        Cursor qc = db.rawQuery(str, null); 
        Log.v(TAG, "HELLO: "+qc.getCount());
        if(qc.getCount()==0){
        	
        	Log.v(TAG,"Empty :--------------------------------------------------");

        ContentResolver cr = getContext().getContentResolver();
        Uri mUri = buildUri("content","edu.buffalo.cse.cse486586.simpledht.provider");
        ContentValues cv1 = new ContentValues();
        cv1 = new ContentValues();
        cv1.put("key", portStr);
        cv1.put("value", myhashedvalue);
        db.insertWithOnConflict("Key_value_Pair", null,cv1, SQLiteDatabase.CONFLICT_IGNORE);
		
        }
	
		else
		{
		if(qc.getCount()>0)	
		{
			
			 
	                 
			Log.i(TAG,"Node recovered from failure phase and total elements inserted: "+qc.getCount());
			
			//delete its database
			db.delete("key_value_pair"," key!= "+portStr , null);
			
			

			
			
            String khash=null;

             
             if(portStr.equals("5562"))
             {
            	 
                 String matrix[] = new String[] { "key", "value" };
                 MatrixCursor m = new MatrixCursor(matrix);
                        Log.i(TAG,"### ENTERED INTO PORTSTR: "+portStr);
                         for (int z = 0; z < 2; z++) 
                         {
                             data = null;
                             isDataNotReceived = true;
                             String iden = "~~" + port_avd; 
                             if(z==0)
                             new ClientTask().executeOnExecutor(threadpoolExecutor,iden,"11112");
                             
                             if(z==1)
                             new ClientTask().executeOnExecutor(threadpoolExecutor,iden,"11120");
                             
                            
                             while (isDataNotReceived)
                             {
                            	

                             }
                             Log.i(TAG,"Data fetched from 11112/11120: next split each key and value:::  "+data);
                             if(data!=null)
                             {
                             String[] star = data.split("---");
                             String keyy = null;
                             String vall = null;
                             for (int l = 0; l < star.length; l++)
                             {
                                 String[] kv = star[l].split(",");
                                 keyy = kv[0];
                                 vall = kv[1];
                                 Log.i(port_avd,"<---### printing each key AND value: from the data fetched in ::  "+keyy+"---"+vall);
                                 String matrix11[] = new String[] { keyy, vall };

                                 m.addRow(matrix11);

                             }
                             }

                         }

                         data = null;
                         isDataNotReceived = true;
                         Log.v(TAG,"M.GETCOUNT(): "+m.getCount());
                         m.moveToFirst();
                         for(int j=0;j<m.getCount();j++)
                         {
                        	 
                        	String keyfinal=m.getString(0);
                        	String valuefinal=m.getString(1);
           Log.i(TAG,"KEY EXTRACTED FROM MATRIX CURSOR OF 5562:::"+keyfinal);             	
                        	try {
                				khash = genHash(keyfinal);
                			} catch (NoSuchAlgorithmException e) {
                				// TODO Auto-generated catch block
                				e.printStackTrace();
                			}
                        	
                        	//Log.v(TAG,"\nkey and value pairs are:$$$$ \n "+keyfinal+"\n"+valuefinal);
                    boolean b=(khash.compareTo(porthash.get(2))>0)&& (khash.compareTo(porthash.get(4))<=0);
            	 if ((khash.compareTo(porthash.get(0)) <= 0|| 
            			 khash.compareTo(porthash.get(sizequ - 1)) > 0)|| b) 
            	 {
                     Log.i(TAG,"finally KEY inserted into  5562:::"+keyfinal);             	

            		// Log.i(TAG,"In line 341:  inserting key:  "+keyfinal );
            		 ContentResolver cr = getContext().getContentResolver();
                     Uri mUri = buildUri("content","edu.buffalo.cse.cse486586.simpledht.provider");
                     ContentValues cv1 = new ContentValues();
                     cv1 = new ContentValues();
                     cv1.put("key", keyfinal);
                     cv1.put("value", valuefinal);
                     db.insertWithOnConflict("Key_value_Pair", null,cv1, SQLiteDatabase.CONFLICT_IGNORE);	
                     

            	 }
            	 
            	 m.moveToNext();
             }
             }
             else if(portStr.equals("5556"))
             {
                 Log.i(TAG,"### ENTERED INTO PORTSTR: "+portStr);

            	 
            	 String matrix[] = new String[] { "key", "value" };
                 MatrixCursor m = new MatrixCursor(matrix);
                         
                         for (int z = 0; z < 2; z++) {
                             data = null;
                             isDataNotReceived = true;
                             String iden = "~~" + port_avd; 
                             if(z==0)
                             new ClientTask().executeOnExecutor(threadpoolExecutor,iden,"11108");
                             
                             if(z==1)
                             new ClientTask().executeOnExecutor(threadpoolExecutor,iden,"11124");

                             
                             while (isDataNotReceived) {
                           
                                
                             }
                             Log.i(TAG,"Data fetched from 11108/11124: next split each key and value:::  "+data);
      

                             if(data!=null)
                             {

                             String[] star = data.split("---");
                             String keyy = null;
                             String vall = null;
                             for (int l = 0; l < star.length; l++) {
                                 String[] kv = star[l].split(",");
                                 keyy = kv[0];
                                 vall = kv[1];
                                 
                                 Log.i(port_avd,"<---### printing each key AND value: from the data fetched in ::  "+keyy+"---"+vall);

                                 String matrix11[] = new String[] { keyy, vall };

                                 m.addRow(matrix11);

                             }
                             }
                         }

                         data = null;
                         isDataNotReceived = true;
                         Log.v(TAG,"M.GETCOUNT(): "+m.getCount());

                         m.moveToFirst();
                         
                         for(int j=0;j<m.getCount();j++)
                         {
                        	 
                        	String keyfinal=m.getString(0);
                        	String valuefinal=m.getString(1); 
                            Log.i(TAG,"KEY EXTRACTED FROM MATRIX CURSOR OF 5556:::"+keyfinal);             	
  	
                        	try {
                				khash = genHash(keyfinal);
                			} catch (NoSuchAlgorithmException e) {
                				// TODO Auto-generated catch block
                				e.printStackTrace();
                			}
                        	
   Log.i(TAG, keyfinal + "Compare Values "+ listport.get(0)+ " "+ listport.get(1)+
		   			listport.get(3) + listport.get(4));
   boolean a1=(khash.compareTo(porthash.get(3)) > 0)&& (khash.compareTo(porthash.get(4))<=0);
   boolean b1=khash.compareTo(porthash.get(0)) <= 0 || khash.compareTo(porthash.get(sizequ - 1)) > 0;
   boolean c1= (khash.compareTo(porthash.get(0))>0 && khash.compareTo(porthash.get(1))<=0);
         Log.i(TAG,"a1---b1---c1: "+a1+"---"+b1+"----"+c1+" AND IT'S KEY: "+keyfinal);
    
   if(a1||b1||c1)
   {
	   
         Log.i(TAG,"finally KEY inserted into  5556:::"+keyfinal);             	

    	  ContentResolver cr = getContext().getContentResolver();
    	  Uri mUri = buildUri("content","edu.buffalo.cse.cse486586.simpledht.provider");
    	 ContentValues cv1 = new ContentValues();
    	 cv1 = new ContentValues();
    	  cv1.put("key", keyfinal);
    	 cv1.put("value", valuefinal);
         db.insertWithOnConflict("Key_value_Pair", null,cv1, SQLiteDatabase.CONFLICT_IGNORE);	
         
            	 
    		 } 
             m.moveToNext();
            	 
             }
             }
             
             else if(portStr.equals("5554"))
             {
            	 
                 Log.i(TAG,"### ENTERED INTO PORTSTR: "+portStr);

            	 String matrix[] = new String[] { "key", "value" };
                 MatrixCursor m = new MatrixCursor(matrix);
                         
                         for (int z = 0; z < 2; z++) {
                             data = null;
                             isDataNotReceived = true;
                             String iden = "~~" + port_avd; 
                             if(z==0)
                             new ClientTask().executeOnExecutor(threadpoolExecutor,iden,"11116");
                             
                             if(z==1)
                             new ClientTask().executeOnExecutor(threadpoolExecutor,iden,"11112");

                             long start = System.currentTimeMillis();
                             long end;
                             long diff;
                             while (isDataNotReceived) {
                            	 
                            	 /*end = System.currentTimeMillis();
                            	 diff=end-start;
                           	 Log.w("timedif","time diff: "+diff);
                            if((diff)>1500)
                           	  {
                           		  Log.i(TAG,"END-START: "+(end-start));
                           		  break;
                           	  }*/
                               
                             }
                             Log.i(TAG,"Data fetched from 11116/11112: next split each key and value:::  "+data);

                             
                             if(data!=null)
                             {
                             String[] star = data.split("---");
                             String keyy = null;
                             String vall = null;
                             for (int l = 0; l < star.length; l++) {
                                 String[] kv = star[l].split(",");
                                 keyy = kv[0];
                                 vall = kv[1];
                                 
                                 Log.i(port_avd,"<---### printing each key AND value: from the data fetched in ::  "+keyy+"---"+vall);

                                 String matrix11[] = new String[] { keyy, vall };

                                 m.addRow(matrix11);

                             }

                         }}

                         data = null;
                         isDataNotReceived = true;
                         Log.v(TAG,"M.GETCOUNT(): "+m.getCount());

                         m.moveToFirst();
                         
                         for(int j=0;j<m.getCount();j++)
                         {
                        	 
                        	String keyfinal=m.getString(0);
                        	String valuefinal=m.getString(1); 
                         
                            Log.i(TAG,"KEY EXTRACTED FROM MATRIX CURSOR OF 5554:::"+keyfinal);             	
                        	
                        	try {
                				khash = genHash(keyfinal);
                			} catch (NoSuchAlgorithmException e) {
                				// TODO Auto-generated catch block
                				e.printStackTrace();
                			}

                 if ((khash.compareTo(porthash.get(0)) <= 0|| 
                		 khash.compareTo(porthash.get(sizequ - 1)) > 0)|| 
                		 (khash.compareTo(porthash.get(0))>0 && khash.compareTo(porthash.get(2))<=0))

                 {
                	 
                     Log.i(TAG,"finally KEY inserted into  5554:::"+keyfinal);             	

                	 

                	 ContentResolver cr = getContext().getContentResolver();
                	  Uri mUri = buildUri("content","edu.buffalo.cse.cse486586.simpledht.provider");
                	 ContentValues cv1 = new ContentValues();
                     cv1 = new ContentValues();
                     cv1.put("key", keyfinal);
                     cv1.put("value", valuefinal);
                     db.insertWithOnConflict("Key_value_Pair", null,cv1, SQLiteDatabase.CONFLICT_IGNORE);		 
                	 

                 }
                 m.moveToNext();
            	 
                         }
             }
             
             else if(portStr.equals("5558"))
             {
            	 
                 Log.i(TAG,"### ENTERED INTO PORTSTR: "+portStr);

            	 String matrix[] = new String[] { "key", "value" };
                 MatrixCursor m = new MatrixCursor(matrix);
                         
                         for (int z = 0; z < 2; z++) {
                             data = null;
                             isDataNotReceived = true;
                             String iden = "~~" + port_avd; 
                             if(z==0)
                             new ClientTask().executeOnExecutor(threadpoolExecutor,iden,"11108");
                             
                             if(z==1)
                             new ClientTask().executeOnExecutor(threadpoolExecutor,iden,"11120");

                             long start = System.currentTimeMillis();
                             long end,diff;
                             while (isDataNotReceived) {
                            	 
                            	 /*end = System.currentTimeMillis();
                            	 diff=end-start;
                              	  Log.w("timedif","time diff: "+diff);
                           	  
                           	  if((diff)>1500)
                           	  {
                           		  Log.i(TAG,"END-START: "+(end-start));
                           		  break;
                           	  }*/

                             }
                             
                             Log.i(TAG,"Data fetched from 11108/11120: next split each key and value:::  "+data);
    
                             if(data!=null)
                             {

                             String[] star = data.split("---");
                             String keyy = null;
                             String vall = null;
                             for (int l = 0; l < star.length; l++) {
                                 String[] kv = star[l].split(",");
                                 keyy = kv[0];
                                 vall = kv[1];
                                 Log.i(port_avd,"<---### printing each key AND value: from the data fetched in ::  "+keyy+"---"+vall);

                                 String matrix11[] = new String[] { keyy, vall };

                                 m.addRow(matrix11);

                             }
                             }
                         }

                         data = null;
                         isDataNotReceived = true;
                         m.moveToFirst();
                         Log.v(TAG,"M.GETCOUNT(): "+m.getCount());

                         for(int j=0;j<m.getCount();j++)
                         {
                        	 
                        	String keyfinal=m.getString(0);
                        	String valuefinal=m.getString(1); 
                        	
                            Log.i(TAG,"KEY EXTRACTED FROM MATRIX CURSOR OF 5558:::"+keyfinal);             	

                        	try {
                				khash = genHash(keyfinal);
                			} catch (NoSuchAlgorithmException e) {
                				// TODO Auto-generated catch block
                				e.printStackTrace();
                			}
if ( (khash.compareTo(porthash.get(0))>0) && (khash.compareTo(porthash.get(3))<=0))
	 
                 {
    Log.i(TAG,"finally KEY inserted into  5558:::"+keyfinal);             	

	ContentResolver cr = getContext().getContentResolver();
	Uri mUri = buildUri("content","edu.buffalo.cse.cse486586.simpledht.provider");
	ContentValues cv1 = new ContentValues();
    cv1 = new ContentValues();
    cv1.put("key", keyfinal);
    cv1.put("value", valuefinal);
    db.insertWithOnConflict("Key_value_Pair", null,cv1, SQLiteDatabase.CONFLICT_IGNORE);		 
	 	

                 }

      m.moveToNext();
            	 
                         } 
            	 
             }
             
             else
             {
            	 
            	 if(portStr.equals("5560"))
            	 {
            		 
                     Log.i(TAG,"### ENTERED INTO PORTSTR: "+portStr);

            		 String matrix[] = new String[] { "key", "value" };
                     MatrixCursor m = new MatrixCursor(matrix);
                             
                             for (int z = 0; z < 2; z++) {
                                 data = null;
                                 isDataNotReceived = true;
                                 String iden = "~~" + port_avd; 
                                 if(z==0)
                                 new ClientTask().executeOnExecutor(threadpoolExecutor,iden,"11116");
                                 
                                 if(z==1)
                                 new ClientTask().executeOnExecutor(threadpoolExecutor,iden,"11124");

                                 long start = System.currentTimeMillis();
                                 long end,diff;
                                 while (isDataNotReceived) {
                                	 
                                	/* end = System.currentTimeMillis();
                                	 diff=end-start;
                                  	  Log.w("timedif","time diff: "+diff);
                               	  
                              	  if((diff)>1500)
                               	  {
                               		  Log.i(TAG,"END-START: "+(end-start));
                               		  break;
                               	  }  */

                                 }
                                 
                                 Log.i(TAG,"Data fetched from 11116/11124: next split each key and value:::  "+data);
                                   if(data!=null)
                                   {
                                 String[] star = data.split("---");
                                 String keyy = null;
                                 String vall = null;
                                 for (int l = 0; l < star.length; l++) {
                                     String[] kv = star[l].split(",");
                                     keyy = kv[0];
                                     vall = kv[1];
                                     
                                     Log.i(port_avd,"<---### printing each key AND value: from the data fetched in ::  "+keyy+"---"+vall);

                                     String matrix11[] = new String[] { keyy, vall };

                                     m.addRow(matrix11);

                                 }
                                   }
                             }

                             data = null;
                             isDataNotReceived = true;
                             Log.v(TAG,"M.GETCOUNT(): "+m.getCount());

                             m.moveToFirst();
                             
                             for(int j=0;j<m.getCount();j++)
                             {
                            	 
                            	String keyfinal=m.getString(0);
                            	String valuefinal=m.getString(1); 
                            	
                                Log.i(TAG,"KEY EXTRACTED FROM MATRIX CURSOR OF 5560:::"+keyfinal);             	

                            	try {
                    				khash = genHash(keyfinal);
                    			} catch (NoSuchAlgorithmException e) {
                    				// TODO Auto-generated catch block
                    				e.printStackTrace();
                    			}
            		 
            		 if ( (khash.compareTo(porthash.get(1))>0) && (khash.compareTo(porthash.get(4))<=0))
            		 {
            			    Log.i(TAG,"finally KEY inserted into  5560:::"+keyfinal);             	

            			  ContentResolver cr = getContext().getContentResolver();
            			  Uri mUri = buildUri("content","edu.buffalo.cse.cse486586.simpledht.provider");
            			  ContentValues cv1 = new ContentValues();
            			    cv1 = new ContentValues();
            			    cv1.put("key", keyfinal);
            			    cv1.put("value", valuefinal);
            			    db.insertWithOnConflict("Key_value_Pair", null,cv1, SQLiteDatabase.CONFLICT_IGNORE);		 
            				 	 

            			 
            		 }
            		 
            		 m.moveToNext();
            		 
                             }
            	 }
            	 
            	 
             }
    			
         
             
		
             
			
			
			
		}
			
		}
		
		
		return true;
	}
	
	
	class ServerTask extends AsyncTask<ServerSocket, String, Void> {

    @Override
   protected Void doInBackground(ServerSocket... sockets) {
       
       ServerSocket serverSocket = sockets[0];
       Socket clientSocket;
       InputStreamReader inputStreamReader;
      BufferedReader bufferedReader;
       String message ="";
       
     
     if(serverSocket != null)
       while (true) {
           try {
              
               Log.v(TAG,"Entered while loop of server class of port number: "+port_avd);
               clientSocket = serverSocket.accept(); 
               inputStreamReader = new InputStreamReader(clientSocket.getInputStream());
               bufferedReader = new BufferedReader(inputStreamReader); //getting the client message
              Log.v(TAG,"reached till buffered reader: --- blocking call ");
               message = bufferedReader.readLine();                                 
                Log.v(TAG,"-----Message received from client task:-----  "+message);
               if(message!=null){      	   
            	   
            	   if(message.contains("^^")) {    		   
            		   
                      Log.v(TAG,"-------------Reached ^^ insert:-------- ");
                      Log.v(TAG,"Message Recived for insert: "+message);
                   String splitt[] = message.split("\\^\\^");
                   String key99 = splitt[0];
                   String value99 = splitt[1];
                   Log.v(TAG,"In line 348: with key: "+key99+"  value: "+value99);
                  ContentResolver cr = getContext().getContentResolver();
                    Uri mUri = buildUri("content","edu.buffalo.cse.cse486586.simpledht.provider");
                   ContentValues cv1 = new ContentValues();
                   Log.v(TAG,"-------------Reached content provider insert:-------- ");
                   cv1 = new ContentValues();
                   cv1.put("key", key99);
                   cv1.put("value", value99);
                  String str = "select * from "+ DBCreation.TABLE_NAME + " where "+ DBCreation.COLUMN_KEY + "='"+ key99 + "'";
                  
                  Cursor qc = db.rawQuery(str, null);
           	   Log.v(TAG,"Versioning  qc.getcount(): "+qc.getCount());                 
              
           	     db.insertWithOnConflict("Key_value_Pair", null,cv1, SQLiteDatabase.CONFLICT_IGNORE);
           	   Log.v(TAG,  "Key: ---value inserted are  "+ key99+"---"+value99);
                   
                   
                 
               }
               
               else if (message.contains("---")) 
               {
                   data = message;
                   isDataNotReceived = false;
               }

               else if (message.contains("~~")) {

                   String portn = message.substring(2,
                           message.length());
                   Log.v(TAG,"PORTN VALUE: in 335 "+portn);

                 //  String str = "select * from "+ DBCreation.TABLE_NAME;
       String str = "select * from "+ DBCreation.TABLE_NAME + " where "+ DBCreation.COLUMN_KEY + "!='"+ portStr + "'";
                   Cursor qc = db.rawQuery(str, null);
                   
                   Log.i(TAG,"In Server :"+portStr+" printing str----->: "+str);
                   String tempkeyval ="";
                   Log.v(TAG,"QC.GETCOUNT(): "+ qc.getCount());                 
                                 	   
                   for (int p1 = 0; p1 < qc.getCount() - 1; p1++) {
                       qc.moveToNext();
                       Log.i(portn,"@@@@---key AND value sending from dump: "+port_avd+" to requested port: "+portn+" is---> " +qc.getString(0)+"----"+qc.getString(1));
                       tempkeyval += (qc.getString(0)+ ","+qc.getString(1));
                       tempkeyval += "---";

                   }

                   if(qc.getCount() >0 ){
                       qc.moveToNext();
                   Log.i(portn,"@@@@---last key AND value sending from dump: "+port_avd+" to requested port: "+portn+" is---> " +qc.getString(0)+"----"+qc.getString(1));
                    
                   tempkeyval += (qc.getString(0) + "," + qc.getString(1));
               		}
                   else{
                	   tempkeyval = "---";
                   }
                   
                   new ClientTask().executeOnExecutor(threadpoolExecutor, tempkeyval,portn);
                  

               }
               
               
               
               else if (message.contains("^%")) {
                   data = message;
                   isDataNotReceived = false;
                   //return null;
               }
               else if (message.contains("##")) {
            	   
                   Log.v(TAG,"IN LINE 413: ##");
                   String array112[] = message.split("\\#\\#");
                   String portnu = array112[1];
                   String keywant = array112[0];
                   Log.v(TAG,"In line 342: "+ portnu+ "  "+keywant);
            Log.v(TAG,"IN LINE 416: ##");                  
            String str = "select * from "+ DBCreation.TABLE_NAME + " where "+ DBCreation.COLUMN_KEY + "='"+ keywant + "'";
                   Cursor qc = db.rawQuery(str, null);
                   qc.moveToFirst();
                   Log.v(TAG,"In line 345");
                   String keyfinal = qc.getString(0);
                   Log.v(TAG,"In line 347");
                   String valuefinal = qc.getString(1);
                   String keval = keyfinal + "^%" + valuefinal;
                   new ClientTask().executeOnExecutor(threadpoolExecutor, keval,portnu);
               }
               
               }
               
               inputStreamReader.close();
               clientSocket.close();

           } catch (IOException ex) {
               System.out.println("Error in reading message");
           }
       }
      
       return null;
   }
        
        
}
	
	class ClientTask extends AsyncTask<String, Void, Void> {
                @Override
                protected Void doInBackground(String... msgs) {
                    try {
                       
                      Socket socket;                     
                      PrintWriter  printwriter;                    
                       String remotePort=msgs[1];                 
                      String msgToSend=msgs[0];                     
             socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),Integer.parseInt(remotePort));
                        printwriter = new PrintWriter(socket.getOutputStream(),true);
                        Log.v(TAG,"In client task msgs to send: "+msgs[0]+" msgs[1]: to server of "+msgs[1]);

                        printwriter.write(msgToSend);                       
                        printwriter.flush();
                        printwriter.close();                        
                        socket.close();
                        
           
          
          
                    } 
                    catch (SocketException e) {
                        Log.e(TAG, "ClientTask UnknownHostException detected "+e);
                    } catch (UnknownHostException e) {
                        Log.e(TAG, "ClientTask UnknownHostException: "+e);
                    } 
                    
                    catch (IOException e) {
                        System.out.println();
                        Log.v(TAG,"In port "+portStr+"  Cannot connect to: "+msgs[1]);
                        Log.e(TAG, "ClientTask socket IOException :"+e);
                        isDataNotReceived = false;
                    }
                   
                    return null;
                }
            }
	@Override
	public synchronized Cursor query(Uri uri, String[] projection, String selection,
			String[] selectionArgs, String sortOrder) {
		// TODO Auto-generated method stub
		
		String keyreceived = selection; 
		Log.v(TAG, "key is "+ keyreceived);
        String keyhash = null;
        int sizequ=listport.size();
        String idenavd=null;
        String portnum1=null;
        String scuportavd=null;
        String sucvalue=null;
       // String regkey = keyreceived + "##" + port_avd;


        try {
            // calculate hash value the key received
            keyhash = genHash(selection);
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        
        if ((!selection.equals("*")) && (!selection.equals("@"))) {
        	
            String regkey = keyreceived + "##" + port_avd;

        	 if (keyhash.compareTo(porthash.get(0)) <= 0|| keyhash.compareTo(porthash.get(sizequ - 1)) > 0) {
                 idenavd = listport.get(0);  
                 scuportavd=listport.get(1);
                 Log.v(TAG,"In line 438 with idenavd: "+idenavd);
                 portnum1 = String.valueOf((Integer.parseInt(idenavd)*2));
                 sucvalue= String.valueOf((Integer.parseInt(scuportavd)*2));

                 new ClientTask().executeOnExecutor(threadpoolExecutor,regkey, portnum1);
        	 }
                 
                 else {

                     for (int y = 0; y < porthash.size(); y++) {
                    	 Log.v(TAG,"In line 446: with Y value: "+y);
                         if ((y + 1) != (porthash.size())) {
      if (keyhash.compareTo(porthash.get(y)) > 0 && keyhash.compareTo(porthash.get(y + 1)) <= 0) 
      {
                     idenavd = listport.get(y+1);
                     if(y+1!=4)
                     {
                     scuportavd=listport.get(y+2);
                     }
                     else
                     {
                    scuportavd=listport.get(0);	 
                     }
                     portnum1 = String.valueOf((Integer.parseInt(idenavd) * 2));
                     sucvalue= String.valueOf((Integer.parseInt(scuportavd)*2));

                     new ClientTask().executeOnExecutor(threadpoolExecutor,regkey,portnum1);
                          break;
                             }
                         }
                     }
                     //return null;
                 }
        	
        	 long start = System.currentTimeMillis();
             long end,diff,count=0;
             while (isDataNotReceived) {
            	 
            	end = System.currentTimeMillis();
            	 diff=end-start;
           	  
          	  if(diff>10000 && count==0)
           	  {
           		  Log.i(TAG,"END-START: "+(end-start));
           		  count++;
                  new ClientTask().executeOnExecutor(threadpoolExecutor,regkey,sucvalue);  
                  
          	  }  

             }
             String[] s33 = data.split("\\^\\%");
             String key11 = s33[0];
             String val11 = s33[1];
             Log.v(TAG,"Value received in line 461: "+val11);
             while(val11.equals(null))
             {
            	 try {
					Thread.sleep(500);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
            	 data=null;
             	isDataNotReceived=true;
                 
                 new ClientTask().executeOnExecutor(threadpoolExecutor,regkey, portnum1);

            	 while (isDataNotReceived) {

                 }
                 String[] s333 = data.split("\\^\\%");
                 key11 = s333[0];
                 val11 = s333[1];
                 Log.v(TAG,"In line 481 with value: "+val11);
            	 
             }
             
             String matrix[] = new String[] { "key", "value" };
             String matrix11[] = new String[] { key11, val11 };
             MatrixCursor m = new MatrixCursor(matrix);
             m.addRow(matrix11);
             data = null;
             isDataNotReceived = true;
             return m;
         
        	
        }
        
       else if (selection.equals("@")) {
        	
        	Log.v(TAG,"Test @ query: for port: "+portStr);
    		String str = "select * from "+ DBCreation.TABLE_NAME + " where "+ DBCreation.COLUMN_KEY + "!='"+ portStr + "'";

            //String str = "select * from " + DBCreation.TABLE_NAME;
            Cursor qc = db.rawQuery(str, null);
            return qc;

        }

       else {
           if (selection.equals("*")) {
             
           String[] portnum ={ "11108", "11112", "11116", "11120","11124" };
           String matrix[] = new String[] { "key", "value" };
           MatrixCursor m = new MatrixCursor(matrix);
                   
                   for (int z = 0; z < portnum.length; z++) {
                       data = null;
                       isDataNotReceived = true;
                       String iden = "~~" + port_avd;                       
                       new ClientTask().executeOnExecutor(threadpoolExecutor,iden,portnum[z]);

                       long start = System.currentTimeMillis();
                       long end,diff;
                       while (isDataNotReceived) {
                      	 
                      	 end = System.currentTimeMillis();
                      	 diff=end-start;
                     	  
                    	  if((diff)>1500)
                     	  {
                     		  break;
                     	  }  

                       }
                         if(data!=null)
                         {
                       String[] star = data.split("---");
                       String keyy = null;
                       String vall = null;
                       for (int l = 0; l < star.length; l++) {
                           String[] kv = star[l].split(",");
                           keyy = kv[0];
                           vall = kv[1];
                           String matrix11[] = new String[] { keyy, vall };

                           m.addRow(matrix11);

                       }

                   }
                   }
                   data = null;
                   isDataNotReceived = true;
                   return m;
               }
           }

		
		
		
		
		
		
		return null;
	}

	@Override
	public int update(Uri uri, ContentValues values, String selection,
			String[] selectionArgs) {
		// TODO Auto-generated method stub
		return 0;
	}

    private String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }
}
