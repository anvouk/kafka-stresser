import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServer;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.handler.BodyHandler;
import io.vertx.ext.web.handler.LoggerFormat;
import io.vertx.ext.web.handler.LoggerHandler;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Main {
    private static final Logger logger = LoggerFactory.getLogger(Main.class);

    private static KafkaStresser kafkaStresser;

    public static void main(String[] args) {
        String kafkaServers = System.getenv("KAFKA_SERVERS");
        if (kafkaServers == null) {
            logger.error("KAFKA_SERVERS is not set");
            System.exit(1);
        }

        kafkaStresser = new KafkaStresser(kafkaServers);

        Vertx vertx = Vertx.vertx();
        Router api = Router.router(vertx);

        api.post("/start").handler(ctx -> {
            try {
                JSONObject body = new JSONObject(ctx.getBodyAsString());

                int topicsCount = body.getInt("topicsCount");
                int partitionsPerTopic = body.getInt("partitionsPerTopic");

                logger.info("starting a new session with {} topics and {} partitions per topic",
                    topicsCount, partitionsPerTopic);
                kafkaStresser.start(topicsCount, partitionsPerTopic);

                ctx.response().setStatusCode(200).end(status(null));
            } catch (JSONException ex) {
                logger.error("invalid json message format received", ex);
                ctx.response().setStatusCode(400).end(status(ex));
            } catch (Exception ex) {
                logger.error("error", ex);
                ctx.response().setStatusCode(500).end(status(ex));
            }
        });

        api.post("/stop").handler(ctx -> {
            try {
                logger.info("stopping session");
                kafkaStresser.stop();
                ctx.response().setStatusCode(200).end(status(null));
            } catch (Exception ex) {
                logger.error("error", ex);
                ctx.response().setStatusCode(500).end(status(ex));
            }
        });

        Router mainRouter = Router.router(vertx);
        mainRouter.route().handler(BodyHandler.create());
        mainRouter.route().handler(LoggerHandler.create(LoggerFormat.TINY));
        mainRouter.route().consumes("application/json").produces("application/json");
        mainRouter.route().handler(event -> {
            event.response()
                .putHeader("Content-Type", "application/json")
                .putHeader("Accept-Encoding", "gzip,deflate");
            event.next();
        });
        mainRouter.mountSubRouter("/api", api);

        logger.info("starting http server");
        HttpServer server = vertx.createHttpServer().requestHandler(mainRouter);
        server.listen(8080);
    }

    private static String status(Throwable throwable) {
        JSONObject root = new JSONObject();
        if (throwable == null) {
            root.put("status", "success");
        } else {
            root.put("status", "failure");
            root.put("reason", throwable.getMessage());
        }
        return root.toString(4);
    }
}
