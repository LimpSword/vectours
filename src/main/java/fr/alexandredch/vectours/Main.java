package fr.alexandredch.vectours;

import fr.alexandredch.vectours.data.SearchParameters;
import fr.alexandredch.vectours.data.Vector;
import fr.alexandredch.vectours.store.base.InMemoryStore;
import io.javalin.Javalin;

public final class Main {

    public static void main(String[] args) {
        InMemoryStore store = new InMemoryStore();
        store.initFromDisk();

        Javalin.create()
                .post("/vectors", ctx -> {
                    Vector vector = ctx.bodyAsClass(Vector.class);
                    store.insert(vector);
                    ctx.status(201);
                })
                .get("/vectors/{id}", ctx -> {
                    String id = ctx.pathParam("id");
                    Vector vector = store.getVector(id);
                    if (vector != null) {
                        ctx.json(vector);
                    } else {
                        ctx.status(404);
                    }
                })
                .post("/search", ctx -> {
                    SearchParameters params = ctx.bodyAsClass(SearchParameters.class);
                    var results = store.search(params);
                    ctx.json(results);
                })
                .start(7001);
    }
}
