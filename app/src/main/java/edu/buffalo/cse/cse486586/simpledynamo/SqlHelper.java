package edu.buffalo.cse.cse486586.simpledynamo;

import android.content.Context;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;

/**
 * Created by amogh on 2/17/15.
 */

public class SqlHelper extends SQLiteOpenHelper{

    //DAO
    //Referred websites tutorialpoint.com, vogella.com and stackoverflow examples to understand how to use SQLite

    public static final String TABLE_NAME= "ContentProviderTable";
    public static final String Keyval="key";
    public static final String ValueCol="value";
    private static final String DATABASE_NAME= "Providerdb";

    public SqlHelper(Context context){
        super(context,DATABASE_NAME,null,1);
    }
    @Override
    public void onCreate(SQLiteDatabase Db){
        Db.execSQL("create table " + TABLE_NAME +"(" + Keyval + " text primary key, " + ValueCol + " text not null, UNIQUE (key) ON CONFLICT REPLACE);");
    }
    @Override
    public void onUpgrade(SQLiteDatabase Db,int oldV, int newV){

    }

}