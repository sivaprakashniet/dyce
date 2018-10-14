package services

import com.typesafe.config.ConfigFactory

object AmplifyStatus {
  private val propConf = ConfigFactory.load()
  val amplifyr_status_upload_stated = propConf.getString("amplifyr_status.upload_stated")
  val amplifyr_status_upload_completed = propConf.getString("amplifyr_status.upload_completed")
  val amplifyr_status_univariate_started = propConf.getString("amplifyr_status.univariate_started")
  val amplifyr_status_univariate_completed = propConf.getString("amplifyr_status.univariate_completed")
  val amplifyr_status_univariate_error = propConf.getString("amplifyr_status.univariate_error")
  val amplifyr_status_preprocessing_started = propConf.getString("amplifyr_status.preprocessing_started")
  val amplifyr_status_preprocessing_completed = propConf.getString("amplifyr_status.preprocessing_completed")
  val amplifyr_status_preprocessing_error = propConf.getString("amplifyr_status.preprocessing_error")
  val amplifyr_status_job_upload = propConf.getString("amplifyr_status.job_upload")
  val amplifyr_status_job_preprocessing = propConf.getString("amplifyr_status.job_preprocessing")
  val amplifyr_status_job_univariate = propConf.getString("amplifyr_status.job_univariate")

}