package forex.logging

import io.circe.{ Json, JsonObject }

object LogEvent {
  def apply(message: String, fields: (String, Json)*): String =
    Json
      .fromJsonObject(
        JsonObject.fromIterable(
          List(
            "message" -> Json.fromString(message),
            "data"    -> Json.fromJsonObject(JsonObject.fromIterable(fields))
          )
        )
      )
      .noSpaces
}
