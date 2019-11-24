package nl.kransen.filestream

import java.io.File
import java.util.concurrent.TimeUnit

import cats.effect._
import cats.implicits._
import monix.eval._
import monix.reactive.{Consumer, Observable}

import scala.concurrent.duration.FiniteDuration

object ScanDir extends TaskApp {

  // Needs enabling the "localContextPropagation" option for TaskLocal variable
  override protected def options: Task.Options = Task.defaultOptions.enableLocalContextPropagation

  def run(args: List[String]): Task[ExitCode] = {
    fileStream().doOnNext(processFile).consumeWith(Consumer.complete).as(ExitCode.Success)
  }

  def fileStream(): Observable[File] = {
    Observable.intervalAtFixedRate(FiniteDuration(1, TimeUnit.SECONDS)).flatMap(_ => listFiles())
  }

  def listFiles(): Observable[File] = {
    Observable.fromIterable(new File("/tmp").listFiles())
  }

  def processFile(file: File): Task[Unit] = {
    val empty: Set[File] = Set()
    for {
      state <- TaskLocal(empty)
      processed <- state.read
      _ <- processNew(file, processed)
      _ <- state.write(processed + file)
    } yield ()
  }

  def processNew(file: File, processed: Set[File]): Task[Unit] = {
    if (!processed.contains(file)) {
      process(file)
    } else {
      Task.unit
    }
  }

  def process(file: File): Task[Unit] = {
    Task(println(file))
  }
}

