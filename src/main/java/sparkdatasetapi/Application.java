package sparkdatasetapi;

public class Application {

    public static void main(String[] args) {
//		InferCSVSchema parser = new InferCSVSchema(); // We're not defining/modelling the schema
//		parser.printSchema();

//		DefineCSVSchema parser2 = new DefineCSVSchema(); // We're defining the schema for the input file
//		parser2.printDefinedSchema();

        JSONLinesParser parser3 = new JSONLinesParser();
        parser3.parseJsonLines();
    }

}
