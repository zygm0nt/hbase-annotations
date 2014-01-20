package pl.touk.hadoop.hbase;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.expression.Expression;
import org.springframework.expression.ExpressionParser;
import org.springframework.expression.spel.standard.SpelExpressionParser;

import pl.touk.hadoop.hbase.annotation.Column;
import pl.touk.hadoop.hbase.annotation.Id;
import pl.touk.hadoop.hbase.annotation.Timestamp;

public class AnnotationDrivenRowMapper<T> {

    private Class<T> targetClass;

    private Logger log = LoggerFactory.getLogger(AnnotationDrivenRowMapper.class);

    private ExpressionParser parser = new SpelExpressionParser();
    
    public AnnotationDrivenRowMapper(Class<T> targetClass) {
        this.targetClass = targetClass;
    }

    @SuppressWarnings("unchecked")
    public T map(Result result) throws Exception {
        Constructor constructor = targetClass.getConstructor();
        T object = (T) constructor.newInstance();
        for (Method method : targetClass.getDeclaredMethods()) {
            byte[] fieldValue = null;
            
            Column column = method.getAnnotation(Column.class);
            if (column  != null) {
                fieldValue = result.getValue(familyName(column), columnName(column));
            }
            Id id = method.getAnnotation(Id.class);
            if (id != null) {
                fieldValue = result.getRow();
            }
            Timestamp timestamp = method.getAnnotation(Timestamp.class);
            if (timestamp != null) {
                fieldValue = Bytes.toBytes(result.raw()[0].getTimestamp() / 1000);
            }

            if (fieldValue != null && fieldValue.length > 0) {
                Value elAnnotation = method.getAnnotation(Value.class);
                Object value = processEl(typeConverter(fieldValue, method.getParameterTypes()[0]), elAnnotation);
                log.debug("calling method " + method.getName() + " with param " + value);
                method.invoke(object, value);
            }
        }
        return object;
    }

    private Object processEl(Object o, Value elAnnotation) {
        if (elAnnotation == null) return o;

        Expression expression = parser.parseExpression(elAnnotation.value());
        return expression.getValue(o);
    }

    private Object typeConverter(byte[] fieldValue, Class<?> typeClass) {
        if (fieldValue == null) return null;

        Object retVal = null;
        if (typeClass == String.class) retVal = Bytes.toString(fieldValue);
        else if (typeClass == Long.class || typeClass == long.class) retVal = Bytes.toLong(fieldValue);
        else if (typeClass == boolean.class || typeClass == Boolean.class) retVal = Bytes.toBoolean(fieldValue);
        else if (typeClass == Date.class) retVal = convertToDate(Bytes.toString(fieldValue), "yyyy-MM-dd");
        else if (typeClass == Integer.class || typeClass == int.class) retVal = Bytes.toInt(fieldValue);
        else log.error("Type " + typeClass + " not implemented");
        return retVal;
    }

    /* handle dates like this: Tue Oct 30 18:29:12 CET 2012 */
    private Date convertToDate(String dateString, String format) {
        SimpleDateFormat sdf = new SimpleDateFormat(format);
        try {
            return sdf.parse(dateString);
        } catch (ParseException e) {
            String fallBackFormat = "EEE MMM dd HH:mm:ss z yyyy";
            if (!fallBackFormat.equals(format)) {
                log.error("Parser exeception: " + e.getMessage());
                return convertToDate(dateString, fallBackFormat);
            } else {
                log.error("Parser exeception: " + e);
            }
        }
        return null;
    }


    private byte[] columnName(Column column) {
        String[] tokens = column.value().split(":");
        return Bytes.toBytes(tokens[1]);
    }

    private byte[] familyName(Column column) {
        String[] tokens = column.value().split(":");
        return Bytes.toBytes(tokens[0]);
    }

}

