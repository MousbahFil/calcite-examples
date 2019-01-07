import org.apache.calcite.adapter.java.ReflectiveSchema;
import org.apache.calcite.config.Lex;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Planner;
import org.apache.calcite.tools.Programs;
import org.apache.calcite.tools.RuleSets;

import com.google.common.collect.Lists;

import org.junit.Test;
import utils.PersonSchemaUtil;
import utils.PersonsSchema;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import static org.junit.Assert.assertTrue;

public class SqlRelExpressionTest {

  @Test
  public void canExecuteJdbcStatementsTest() throws SQLException, ClassNotFoundException {
    CalciteConnection calciteConnection = PersonSchemaUtil.createCalciteConnection();
    SchemaPlus rootSchema = calciteConnection.getRootSchema();
    PersonsSchema personsSchema = PersonSchemaUtil.createPeople();
    ReflectiveSchema reflectiveSchema = new ReflectiveSchema(personsSchema);
    rootSchema.add("people", reflectiveSchema);
    executeQueryUsingPlainOldJDBC(calciteConnection);
  }

  @Test
  public void canParseAndValidateQueryTest() throws Exception {
    SchemaPlus rootSchema = PersonSchemaUtil.getRootSchema();
    List<RelTraitDef> traitDefs = Lists.newArrayList(ConventionTraitDef.INSTANCE, RelCollationTraitDef.INSTANCE);

    FrameworkConfig calciteFrameworkConfig = Frameworks.newConfigBuilder()
        .parserConfig(SqlParser.configBuilder()
            .setLex(Lex.MYSQL)
            .build())
        .defaultSchema(rootSchema)
        .traitDefs(traitDefs)
        .context(Contexts.EMPTY_CONTEXT)
        .ruleSets(RuleSets.ofList())
        .costFactory(null)
        .typeSystem(RelDataTypeSystem.DEFAULT)
        .build();

    Planner planner = Frameworks.getPlanner(calciteFrameworkConfig);
    SqlNode sqlNode = planner.parse(
        "Select p.firstName as T from people.persons p inner join people.persons q on p.firstName=q.firstName ");
    SqlNode validatedSqlNode = planner.validate(sqlNode);
    RelNode node = planner.rel(validatedSqlNode).project();
    String string = RelOptUtil.toString(node);
    System.out.println(string);
    assertTrue(node instanceof LogicalProject);
  }

  private static void executeQueryUsingPlainOldJDBC(final CalciteConnection calciteConnection) throws SQLException {
    Statement statement = calciteConnection.createStatement();
    ResultSet resultSet = statement.executeQuery("Select * from people.persons");
    int count = 0;
    while (resultSet.next()) {
      count++;
    }
    System.out.println(count);
    resultSet.close();
    assertTrue(count == 3);
  }

}
