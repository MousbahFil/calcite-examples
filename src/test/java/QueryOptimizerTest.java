import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.adapter.enumerable.EnumerableProject;
import org.apache.calcite.config.Lex;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Planner;
import org.apache.calcite.tools.Program;
import org.apache.calcite.tools.Programs;
import static org.junit.Assert.*;

import org.junit.Test;
import utils.PersonSchemaUtil;

import java.util.List;

public class QueryOptimizerTest {

  @Test
  public void canOptimizeQueryTest() throws Exception {
    runProjectQueryWithLex(
        "Select p.firstName as T from people.persons p inner join people.persons q on p.firstName=q.firstName");
  }


  private static void runProjectQueryWithLex(String sql) throws Exception {
    SqlParser.Config javaLex = SqlParser.configBuilder().setLex(Lex.MYSQL).build();
    Planner planner = getPlanner(null, javaLex, Programs.ofRules(Programs.RULE_SET));
    SqlNode parse = planner.parse(sql);
    SqlNode validate = planner.validate(parse);
    RelNode convert = planner.rel(validate).rel;
    RelTraitSet traitSet = convert.getTraitSet().replace(EnumerableConvention.INSTANCE);
    RelNode transform = planner.transform(0, traitSet, convert);
    System.out.println(RelOptUtil.toString(transform));
    assertTrue(transform instanceof EnumerableProject);
  }

  private static Planner getPlanner(List<RelTraitDef> traitDefs,
      SqlParser.Config parserConfig, Program... programs) throws Exception {
    SchemaPlus rootSchema = PersonSchemaUtil.getRootSchema();
    final FrameworkConfig config = Frameworks.newConfigBuilder()
        .parserConfig(parserConfig)
        .defaultSchema(rootSchema)
        .traitDefs(traitDefs)
        .programs(programs)
        .build();
    return Frameworks.getPlanner(config);
  }


}
