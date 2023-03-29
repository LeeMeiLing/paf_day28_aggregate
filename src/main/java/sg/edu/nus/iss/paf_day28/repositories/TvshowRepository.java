package sg.edu.nus.iss.paf_day28.repositories;

import org.apache.catalina.Pipeline;
import org.bson.Document;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Sort;
import org.springframework.data.domain.Sort.Direction;
import org.springframework.data.mongodb.MongoExpression;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.aggregation.AggregationExpression;
import org.springframework.data.mongodb.core.aggregation.AggregationResults;
import org.springframework.data.mongodb.core.aggregation.GroupOperation;
import org.springframework.data.mongodb.core.aggregation.MatchOperation;
import org.springframework.data.mongodb.core.aggregation.ProjectionOperation;
import org.springframework.data.mongodb.core.aggregation.SortOperation;
import org.springframework.data.mongodb.core.aggregation.UnwindOperation;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.stereotype.Repository;

@Repository
public class TvshowRepository {

    public static final String C_TVSHOWS = "tvshows";
    
    @Autowired
    MongoTemplate mongoTemplate;

    /*
     * db.tvshows.aggregate([
     *  { $match: { type: "a type"} }
     * ])
     */
    public void findTvshowsByType(String type){

        // stages
            // MatchOperation matchType = Aggregation.match(
            //     Criteria.where("type").is(type)
            // );
        MatchOperation matchType = Aggregation.match(
            Criteria.where("type").regex(type,"i")
        );

        // projection
        ProjectionOperation selectFields = Aggregation.project("id", "name", "summary").andExclude("_id");

        // create the pipeline from stages
        Aggregation pipeline = Aggregation.newAggregation(matchType, selectFields);

        // perform the query
        AggregationResults<Document> results = mongoTemplate.aggregate(pipeline, C_TVSHOWS, Document.class);

        for (Document d : results){
            System.out.printf(">>>> %s\n", d.toJson());
        }

    }

    /*
     * db.tvshows.aggregate([
     *   { $group: 
            { 
                _id: "$network.country.timezone" ,
                total_shows : { $sum:1},
                titles: { $push: "$name" }
            }
    }
     * ])
     */
    public void groupShowsByTimezone(){

        // stage
        GroupOperation tzGroup = Aggregation.group("network.country.timezone")
                .count().as("total_shows")
                .push("name").as("titles");

        // create the pipeline
        Aggregation pipeline = Aggregation.newAggregation(tzGroup);

        // perform the query
        AggregationResults<Document> results = mongoTemplate.aggregate(pipeline, C_TVSHOWS, Document.class);

        for (Document d : results){
            System.out.printf(">>>> %s\n", d.toJson());
        }
    }


    /*
     * db.tvshows.aggregate([
        {
            $project: {
                _id: "$_id",
                title: "$name"
            }
        },
        {
            $sort: { title: 1 }
        }
    ])
     */
    public void summarizeTvShows(String type){

        // stage
        MatchOperation filterByType = Aggregation.match(
            Criteria.where("type").regex(type, "i")
        );

        ProjectionOperation summarizeFields = Aggregation.project("_id", "id")
                .and("name").as("title");

        SortOperation orderByTitle = Aggregation.sort(Sort.by(Direction.ASC, "title"));

        Aggregation pipeline = Aggregation.newAggregation(filterByType, summarizeFields, orderByTitle);


        // perform the query
        AggregationResults<Document> results = mongoTemplate.aggregate(pipeline, C_TVSHOWS, Document.class);

        for (Document d : results){
            System.out.printf(">>>> %s\n", d.toJson());
        }
    }

    /*
     * db.tvshows.aggregate([
            {
                $project: {
                    _id: "$_id",
                    title: {  $concat: [ "$name", "(" , {$toString: "$runtime"}, "mins)" ]  }
                }
            },
            {
                $sort: { title: 1 }
            }

        ])
     */
     public void summarizeTvShows2(String type){

        // stages
        MatchOperation filterByType = Aggregation.match(
            Criteria.where("type").regex(type, "i")
        );

        ProjectionOperation summarizeFields = Aggregation.project("id", "type")
                .and(
                    AggregationExpression.from(
                        MongoExpression.create(
                            """
                                $concat: [ "$name", "(" , {$toString: "$runtime"}, "mins)" ]
                            """)
                        )
                    )
                .as("title")
                .andExclude("_id");

        SortOperation orderByTitle = Aggregation.sort(Sort.by(Direction.ASC, "title"));

        // create the pipeline
        Aggregation pipeline = Aggregation.newAggregation(filterByType, summarizeFields, orderByTitle);

        // perform the query
        AggregationResults<Document> results = mongoTemplate.aggregate(pipeline, C_TVSHOWS, Document.class);

        for (Document d : results){
            System.out.printf(">>>> %s\n", d.toJson());
        }
    }

    /*
     * db.tvshows.aggregate([

            {
                $unwind: "$genres"
            },
            {
                $group: {
                    _id: "abc",
                    category: { $addToSet: "$genres"}
                }
            }
        ])
     */
    public void showCategory(){

        // stages
        UnwindOperation unwindGenres = Aggregation.unwind("genres");

        GroupOperation groupGenres = Aggregation.group("abc") // give a name or null to force all genre to be grouped tgt
            .addToSet("genres")
            .as("categories");

        // create the pipeline
        Aggregation pipeline = Aggregation.newAggregation(unwindGenres,groupGenres);

        // perform the query
        AggregationResults<Document> results = mongoTemplate.aggregate(pipeline, C_TVSHOWS, Document.class);

        for (Document d : results){
            System.out.printf(">>>> %s\n", d.toJson());
        }

    }
}
