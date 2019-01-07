import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import static org.junit.Assert.*;

import org.junit.Test;

public class SqlParserTest {

  @Test
  public void parseSelectQueryTest() throws SqlParseException {
    SqlParser.ConfigBuilder configBuilder = SqlParser.configBuilder();
    SqlParser sqlParser = SqlParser.create("SELECT * from T", configBuilder.build());
    SqlNode sqlNode = sqlParser.parseStmt();
    assertTrue("Query is not instance of SqlSelect", sqlNode instanceof SqlSelect);
    System.out.println(sqlNode);
  }

}
