// ORM class for table 'dbo.RdClass'
// WARNING: This class is AUTO-GENERATED. Modify at your own risk.
//
// Debug information:
// Generated date: Tue Mar 20 16:30:16 CST 2018
// For connector: org.apache.sqoop.manager.GenericJdbcManager
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.lib.db.DBWritable;
import com.cloudera.sqoop.lib.JdbcWritableBridge;
import com.cloudera.sqoop.lib.DelimiterSet;
import com.cloudera.sqoop.lib.FieldFormatter;
import com.cloudera.sqoop.lib.RecordParser;
import com.cloudera.sqoop.lib.BooleanParser;
import com.cloudera.sqoop.lib.BlobRef;
import com.cloudera.sqoop.lib.ClobRef;
import com.cloudera.sqoop.lib.LargeObjectLoader;
import com.cloudera.sqoop.lib.SqoopRecord;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class dbo_RdClass extends SqoopRecord  implements DBWritable, Writable {
  private final int PROTOCOL_VERSION = 3;
  public int getClassFormatVersion() { return PROTOCOL_VERSION; }
  protected ResultSet __cur_result_set;
  private String defectId;
  public String get_defectId() {
    return defectId;
  }
  public void set_defectId(String defectId) {
    this.defectId = defectId;
  }
  public dbo_RdClass with_defectId(String defectId) {
    this.defectId = defectId;
    return this;
  }
  private String categories;
  public String get_categories() {
    return categories;
  }
  public void set_categories(String categories) {
    this.categories = categories;
  }
  public dbo_RdClass with_categories(String categories) {
    this.categories = categories;
    return this;
  }
  private java.sql.Timestamp loaded_timestamp;
  public java.sql.Timestamp get_loaded_timestamp() {
    return loaded_timestamp;
  }
  public void set_loaded_timestamp(java.sql.Timestamp loaded_timestamp) {
    this.loaded_timestamp = loaded_timestamp;
  }
  public dbo_RdClass with_loaded_timestamp(java.sql.Timestamp loaded_timestamp) {
    this.loaded_timestamp = loaded_timestamp;
    return this;
  }
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof dbo_RdClass)) {
      return false;
    }
    dbo_RdClass that = (dbo_RdClass) o;
    boolean equal = true;
    equal = equal && (this.defectId == null ? that.defectId == null : this.defectId.equals(that.defectId));
    equal = equal && (this.categories == null ? that.categories == null : this.categories.equals(that.categories));
    equal = equal && (this.loaded_timestamp == null ? that.loaded_timestamp == null : this.loaded_timestamp.equals(that.loaded_timestamp));
    return equal;
  }
  public boolean equals0(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof dbo_RdClass)) {
      return false;
    }
    dbo_RdClass that = (dbo_RdClass) o;
    boolean equal = true;
    equal = equal && (this.defectId == null ? that.defectId == null : this.defectId.equals(that.defectId));
    equal = equal && (this.categories == null ? that.categories == null : this.categories.equals(that.categories));
    equal = equal && (this.loaded_timestamp == null ? that.loaded_timestamp == null : this.loaded_timestamp.equals(that.loaded_timestamp));
    return equal;
  }
  public void readFields(ResultSet __dbResults) throws SQLException {
    this.__cur_result_set = __dbResults;
    this.defectId = JdbcWritableBridge.readString(1, __dbResults);
    this.categories = JdbcWritableBridge.readString(2, __dbResults);
    this.loaded_timestamp = JdbcWritableBridge.readTimestamp(3, __dbResults);
  }
  public void readFields0(ResultSet __dbResults) throws SQLException {
    this.defectId = JdbcWritableBridge.readString(1, __dbResults);
    this.categories = JdbcWritableBridge.readString(2, __dbResults);
    this.loaded_timestamp = JdbcWritableBridge.readTimestamp(3, __dbResults);
  }
  public void loadLargeObjects(LargeObjectLoader __loader)
      throws SQLException, IOException, InterruptedException {
  }
  public void loadLargeObjects0(LargeObjectLoader __loader)
      throws SQLException, IOException, InterruptedException {
  }
  public void write(PreparedStatement __dbStmt) throws SQLException {
    write(__dbStmt, 0);
  }

  public int write(PreparedStatement __dbStmt, int __off) throws SQLException {
    JdbcWritableBridge.writeString(defectId, 1 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(categories, 2 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeTimestamp(loaded_timestamp, 3 + __off, 93, __dbStmt);
    return 3;
  }
  public void write0(PreparedStatement __dbStmt, int __off) throws SQLException {
    JdbcWritableBridge.writeString(defectId, 1 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(categories, 2 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeTimestamp(loaded_timestamp, 3 + __off, 93, __dbStmt);
  }
  public void readFields(DataInput __dataIn) throws IOException {
this.readFields0(__dataIn);  }
  public void readFields0(DataInput __dataIn) throws IOException {
    if (__dataIn.readBoolean()) { 
        this.defectId = null;
    } else {
    this.defectId = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.categories = null;
    } else {
    this.categories = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.loaded_timestamp = null;
    } else {
    this.loaded_timestamp = new Timestamp(__dataIn.readLong());
    this.loaded_timestamp.setNanos(__dataIn.readInt());
    }
  }
  public void write(DataOutput __dataOut) throws IOException {
    if (null == this.defectId) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, defectId);
    }
    if (null == this.categories) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, categories);
    }
    if (null == this.loaded_timestamp) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeLong(this.loaded_timestamp.getTime());
    __dataOut.writeInt(this.loaded_timestamp.getNanos());
    }
  }
  public void write0(DataOutput __dataOut) throws IOException {
    if (null == this.defectId) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, defectId);
    }
    if (null == this.categories) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, categories);
    }
    if (null == this.loaded_timestamp) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeLong(this.loaded_timestamp.getTime());
    __dataOut.writeInt(this.loaded_timestamp.getNanos());
    }
  }
  private static final DelimiterSet __outputDelimiters = new DelimiterSet((char) 44, (char) 10, (char) 0, (char) 0, false);
  public String toString() {
    return toString(__outputDelimiters, true);
  }
  public String toString(DelimiterSet delimiters) {
    return toString(delimiters, true);
  }
  public String toString(boolean useRecordDelim) {
    return toString(__outputDelimiters, useRecordDelim);
  }
  public String toString(DelimiterSet delimiters, boolean useRecordDelim) {
    StringBuilder __sb = new StringBuilder();
    char fieldDelim = delimiters.getFieldsTerminatedBy();
    __sb.append(FieldFormatter.escapeAndEnclose(defectId==null?"null":defectId, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(categories==null?"null":categories, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(loaded_timestamp==null?"null":"" + loaded_timestamp, delimiters));
    if (useRecordDelim) {
      __sb.append(delimiters.getLinesTerminatedBy());
    }
    return __sb.toString();
  }
  public void toString0(DelimiterSet delimiters, StringBuilder __sb, char fieldDelim) {
    __sb.append(FieldFormatter.escapeAndEnclose(defectId==null?"null":defectId, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(categories==null?"null":categories, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(loaded_timestamp==null?"null":"" + loaded_timestamp, delimiters));
  }
  private static final DelimiterSet __inputDelimiters = new DelimiterSet((char) 8, (char) 10, (char) 0, (char) 0, false);
  private RecordParser __parser;
  public void parse(Text __record) throws RecordParser.ParseError {
    if (null == this.__parser) {
      this.__parser = new RecordParser(__inputDelimiters);
    }
    List<String> __fields = this.__parser.parseRecord(__record);
    __loadFromFields(__fields);
  }

  public void parse(CharSequence __record) throws RecordParser.ParseError {
    if (null == this.__parser) {
      this.__parser = new RecordParser(__inputDelimiters);
    }
    List<String> __fields = this.__parser.parseRecord(__record);
    __loadFromFields(__fields);
  }

  public void parse(byte [] __record) throws RecordParser.ParseError {
    if (null == this.__parser) {
      this.__parser = new RecordParser(__inputDelimiters);
    }
    List<String> __fields = this.__parser.parseRecord(__record);
    __loadFromFields(__fields);
  }

  public void parse(char [] __record) throws RecordParser.ParseError {
    if (null == this.__parser) {
      this.__parser = new RecordParser(__inputDelimiters);
    }
    List<String> __fields = this.__parser.parseRecord(__record);
    __loadFromFields(__fields);
  }

  public void parse(ByteBuffer __record) throws RecordParser.ParseError {
    if (null == this.__parser) {
      this.__parser = new RecordParser(__inputDelimiters);
    }
    List<String> __fields = this.__parser.parseRecord(__record);
    __loadFromFields(__fields);
  }

  public void parse(CharBuffer __record) throws RecordParser.ParseError {
    if (null == this.__parser) {
      this.__parser = new RecordParser(__inputDelimiters);
    }
    List<String> __fields = this.__parser.parseRecord(__record);
    __loadFromFields(__fields);
  }

  private void __loadFromFields(List<String> fields) {
    Iterator<String> __it = fields.listIterator();
    String __cur_str = null;
    try {
    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.defectId = null; } else {
      this.defectId = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.categories = null; } else {
      this.categories = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.loaded_timestamp = null; } else {
      this.loaded_timestamp = java.sql.Timestamp.valueOf(__cur_str);
    }

    } catch (RuntimeException e) {    throw new RuntimeException("Can't parse input data: '" + __cur_str + "'", e);    }  }

  private void __loadFromFields0(Iterator<String> __it) {
    String __cur_str = null;
    try {
    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.defectId = null; } else {
      this.defectId = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.categories = null; } else {
      this.categories = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.loaded_timestamp = null; } else {
      this.loaded_timestamp = java.sql.Timestamp.valueOf(__cur_str);
    }

    } catch (RuntimeException e) {    throw new RuntimeException("Can't parse input data: '" + __cur_str + "'", e);    }  }

  public Object clone() throws CloneNotSupportedException {
    dbo_RdClass o = (dbo_RdClass) super.clone();
    o.loaded_timestamp = (o.loaded_timestamp != null) ? (java.sql.Timestamp) o.loaded_timestamp.clone() : null;
    return o;
  }

  public void clone0(dbo_RdClass o) throws CloneNotSupportedException {
    o.loaded_timestamp = (o.loaded_timestamp != null) ? (java.sql.Timestamp) o.loaded_timestamp.clone() : null;
  }

  public Map<String, Object> getFieldMap() {
    Map<String, Object> __sqoop$field_map = new TreeMap<String, Object>();
    __sqoop$field_map.put("defectId", this.defectId);
    __sqoop$field_map.put("categories", this.categories);
    __sqoop$field_map.put("loaded_timestamp", this.loaded_timestamp);
    return __sqoop$field_map;
  }

  public void getFieldMap0(Map<String, Object> __sqoop$field_map) {
    __sqoop$field_map.put("defectId", this.defectId);
    __sqoop$field_map.put("categories", this.categories);
    __sqoop$field_map.put("loaded_timestamp", this.loaded_timestamp);
  }

  public void setField(String __fieldName, Object __fieldVal) {
    if ("defectId".equals(__fieldName)) {
      this.defectId = (String) __fieldVal;
    }
    else    if ("categories".equals(__fieldName)) {
      this.categories = (String) __fieldVal;
    }
    else    if ("loaded_timestamp".equals(__fieldName)) {
      this.loaded_timestamp = (java.sql.Timestamp) __fieldVal;
    }
    else {
      throw new RuntimeException("No such field: " + __fieldName);
    }
  }
  public boolean setField0(String __fieldName, Object __fieldVal) {
    if ("defectId".equals(__fieldName)) {
      this.defectId = (String) __fieldVal;
      return true;
    }
    else    if ("categories".equals(__fieldName)) {
      this.categories = (String) __fieldVal;
      return true;
    }
    else    if ("loaded_timestamp".equals(__fieldName)) {
      this.loaded_timestamp = (java.sql.Timestamp) __fieldVal;
      return true;
    }
    else {
      return false;    }
  }
}
