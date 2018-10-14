package oauth

import javax.inject.Inject
import play.api.libs.ws.{WSClient, WSResponse}
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global
import play.api.Configuration

class OAuth2 @Inject()(wSClient: WSClient, configuration: Configuration) {
  val clientId = configuration.underlying.getString("oauth2Config.googleClientId")
  val clientSecret = configuration.underlying.getString("oauth2Config.googleClientSecret")
  val authURI = configuration.underlying.getString("oauth2Config.googleAuthURI")
  val redirectURI = configuration.underlying.getString("oauth2Config.redirectURI")
  val tokenURI = configuration.underlying.getString("oauth2Config.tokenURI")
  val tokenInfoURI = configuration.underlying.getString("oauth2Config.tokenInfoURI")
  val scope = configuration.underlying.getString("oauth2Config.scope")
  val userprofileURI = configuration.underlying.getString("oauth2Config.googleUserprofileURI")
  val logoutURI = configuration.underlying.getString("oauth2Config.googleLogoutURI")
  val clientDomain = configuration.underlying.getString("oauth2Config.googleClientDomain")

  def getAuthorizationUrl(redirectUri: String, state: String): String = {
    val auth_url = authURI + "?client_id=" + clientId + "&redirect_uri=" +
      redirectURI + "&state=" + state + "&scope=" +
      scope + "&response_type=code&hd=" + clientDomain
    auth_url.format(clientId, redirectUri, scope, state)
  }

  def getToken(code: String): Future[String] = {
    val payload = Map(
      "client_id" -> Seq(clientId),
      "client_secret" -> Seq(clientSecret),
      "grant_type" -> Seq("authorization_code"),
      "code" -> Seq(code),
      "redirect_uri" -> Seq(redirectURI))

    val tokenResponse = wSClient.url(tokenURI)
      .withHeaders("Content-type" -> "application/x-www-form-urlencoded")
      .post(payload)
    tokenResponse.flatMap { response =>
      (response.json \ "access_token").asOpt[String]
        .fold(Future.failed[String](new IllegalStateException("invalid_token"))) { accessToken =>
          Future.successful(accessToken)
        }
    }
  }

  def tokenInfo(accessToken: String): Future[Either[String, WSResponse]] = {
    val tokenResponse = wSClient.url(tokenInfoURI + "?access_token=" + accessToken).get()
    tokenResponse.map { response =>
      val user_email = (response.json \ "email").asOpt[String]
      if (response.status == 200) {
        Right(response)
      } else {
        val error = (response.json \ "error_description").asOpt[String]
        Left(error.toString)
      }
    }
  }

  def userInfo(accessToken: String): Future[WSResponse] = {
    wSClient.url(userprofileURI + "?access_token=" + accessToken).get()
  }

  def logout(accessToken: String): Future[WSResponse] = {
    wSClient.url(logoutURI + "?token=" + accessToken).get()
  }
}