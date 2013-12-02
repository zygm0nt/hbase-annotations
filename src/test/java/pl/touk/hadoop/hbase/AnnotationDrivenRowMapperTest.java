package pl.touk.hadoop.hbase;

import org.junit.Assert;
import org.junit.Test;
import org.springframework.expression.Expression;
import org.springframework.expression.ExpressionParser;
import org.springframework.expression.spel.standard.SpelExpressionParser;

/**
 * @author mcl
 */
public class AnnotationDrivenRowMapperTest {
    
    @Test
    public void shouldTestSpringExpressionLanguage() throws Exception {
        String expression = "split('\\^')[0]";

        ExpressionParser parser = new SpelExpressionParser();
        Expression elExpr = parser.parseExpression(expression);
        String result = (String) elExpr.getValue("abc^cde");

        Assert.assertEquals("abc", result);
    }
}
