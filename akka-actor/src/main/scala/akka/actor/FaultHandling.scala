/**
 * Copyright (C) 2009-2014 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.actor

import language.implicitConversions
import java.lang.{ Iterable ⇒ JIterable }
import java.util.concurrent.TimeUnit
import akka.japi.Util.immutableSeq
import scala.collection.mutable.ArrayBuffer
import scala.collection.immutable
import scala.concurrent.duration.Duration
import akka.event.Logging.LogEvent
import akka.event.Logging.Error
import akka.event.Logging.Warning
import scala.util.control.NonFatal

/**
 * INTERNAL API
 */
private[akka] sealed trait ChildStats

/**
 * INTERNAL API
 */
private[akka] case object ChildNameReserved extends ChildStats

/**
 * ChildRestartStats is the statistics kept by every parent Actor for every child Actor
  * ChildRestartStats是由父Actor保管的,其每一个子Actor的统计信息
 * and is used for SupervisorStrategies to know how to deal with problems that occur for the children.
  * 用于监控策略,在子Actor出现问题时的进行处理的依据
 */
case class ChildRestartStats(child: ActorRef, var maxNrOfRetriesCount: Int = 0, var restartTimeWindowStartNanos: Long = 0L)
  extends ChildStats {

  def uid: Int = child.path.uid

  //FIXME How about making ChildRestartStats immutable and then move these methods into the actual supervisor strategies?
  def requestRestartPermission(retriesWindow: (Option[Int], Option[Int])): Boolean =
    retriesWindow match {
            // 设置了重试次数,首先判断重试次数是否大于1,小于1,则直接不能restart,返回false
      case (Some(retries), _) if retries < 1 ⇒ false
          // 如果设置了重试次数,但没有设置窗口,则判断restart次数是否达到阈值
      case (Some(retries), None)             ⇒ { maxNrOfRetriesCount += 1; maxNrOfRetriesCount <= retries }
          // 重试次数与窗口都设置时,进行窗口期判断
      case (x, Some(window))                 ⇒ retriesInWindowOkay(if (x.isDefined) x.get else 1, window)
          // 没有设置重试次数时,则不做重试的阈值限制,直接进行restart
      case (None, _)                         ⇒ true
    }

  private def retriesInWindowOkay(retries: Int, window: Int): Boolean = {
    /*
     * Simple window algorithm: window is kept open for a certain time
     * 简单的窗口算法 : 在restart后的一段时间内,窗口保持打开状态
     * after a restart and if enough restarts happen during this time, it
     * 如果在这段时间内,restart的次数达到阈值,则拒绝重启
     * denies. Otherwise window closes and the scheme starts over.
     * 否则,窗口关闭,所有流程重新来过
     */
    val retriesDone = maxNrOfRetriesCount + 1
    val now = System.nanoTime
      // 获取窗口开启时间
    val windowStart =
      if (restartTimeWindowStartNanos == 0) {
          // 如果上次的开启时间为0,则设置为当前时间
        restartTimeWindowStartNanos = now
        now
      } else restartTimeWindowStartNanos
      // 检查当前窗口期是否有效
    val insideWindow = (now - windowStart) <= TimeUnit.MILLISECONDS.toNanos(window)
    if (insideWindow) {
        // 当前窗口期有效,则判断重试次数是否达到阈值
      maxNrOfRetriesCount = retriesDone
      retriesDone <= retries
    } else {
        // 如果窗口期已经失效,则重启窗口
      maxNrOfRetriesCount = 1
      restartTimeWindowStartNanos = now
      true
    }
  }
}

/**
 * Implement this interface in order to configure the supervisorStrategy for
  * 实现这个接口来对顶层守护actor(/user)进行 监控策略 的配置
 * the top-level guardian actor (`/user`). An instance of this class must be
  * 这个类的构建函数必须是无参的
 * instantiable using a no-arg constructor.
 */
trait SupervisorStrategyConfigurator {
  def create(): SupervisorStrategy
}

final class DefaultSupervisorStrategy extends SupervisorStrategyConfigurator {
  override def create(): SupervisorStrategy = SupervisorStrategy.defaultStrategy
}

final class StoppingSupervisorStrategy extends SupervisorStrategyConfigurator {
  override def create(): SupervisorStrategy = SupervisorStrategy.stoppingStrategy
}

trait SupervisorStrategyLowPriorityImplicits { this: SupervisorStrategy.type ⇒

  /**
   * Implicit conversion from `Seq` of Cause-Directive pairs to a `Decider`. See makeDecider(causeDirective).
   */
  implicit def seqCauseDirective2Decider(trapExit: Iterable[CauseDirective]): Decider = makeDecider(trapExit)
  // the above would clash with seqThrowable2Decider for empty lists
}

object SupervisorStrategy extends SupervisorStrategyLowPriorityImplicits {
  sealed trait Directive

  /**
   * Resumes message processing for the failed Actor
   */
  case object Resume extends Directive

  /**
   * Discards the old Actor instance and replaces it with a new,
   * then resumes message processing.
   */
  case object Restart extends Directive

  /**
   * Stops the Actor
   */
  case object Stop extends Directive

  /**
   * Escalates the failure to the supervisor of the supervisor,
   * by rethrowing the cause of the failure, i.e. the supervisor fails with
   * the same exception as the child.
   */
  case object Escalate extends Directive

  /**
   * Java API: Returning this directive resumes message processing for the failed Actor
   */
  def resume = Resume

  /**
   * Java API: Returning this directive discards the old Actor instance and replaces it with a new,
   * then resumes message processing.
   */
  def restart = Restart

  /**
   * Java API: Returning this directive stops the Actor
   */
  def stop = Stop

  /**
   * Java API: Returning this directive escalates the failure to the supervisor of the supervisor,
   * by rethrowing the cause of the failure, i.e. the supervisor fails with
   * the same exception as the child.
   */
  def escalate = Escalate

  /**
   * When supervisorStrategy is not specified for an actor this
    * 当监控策略没有为一个actor指定时,这个Decider被默认使用
   * [[Decider]] is used by default in the supervisor strategy.
   * The child will be stopped when [[akka.actor.ActorInitializationException]],
   * [[akka.actor.ActorKilledException]], or [[akka.actor.DeathPactException]] is
   * thrown. It will be restarted for other `Exception` types.
   * The error is escalated if it's a `Throwable`, i.e. `Error`.
   */
  final val defaultDecider: Decider = {
    case _: ActorInitializationException ⇒ Stop
    case _: ActorKilledException         ⇒ Stop
    case _: DeathPactException           ⇒ Stop
    case _: Exception                    ⇒ Restart
  }

  /**
   * When supervisorStrategy is not specified for an actor this
   * is used by default. OneForOneStrategy with decider defined in
   * [[#defaultDecider]].
   */
  final val defaultStrategy: SupervisorStrategy = {
    OneForOneStrategy()(defaultDecider)
  }

  /**
   * This strategy resembles Erlang in that failing children are always
   * terminated (one-for-one).
   */
  final val stoppingStrategy: SupervisorStrategy = {
    def stoppingDecider: Decider = {
      case _: Exception ⇒ Stop
    }
    OneForOneStrategy()(stoppingDecider)
  }

  /**
   * Implicit conversion from `Seq` of Throwables to a `Decider`.
   * This maps the given Throwables to restarts, otherwise escalates.
   */
  implicit def seqThrowable2Decider(trapExit: immutable.Seq[Class[_ <: Throwable]]): Decider = makeDecider(trapExit)

  type Decider = PartialFunction[Throwable, Directive]
  type JDecider = akka.japi.Function[Throwable, Directive]
  type CauseDirective = (Class[_ <: Throwable], Directive)

  /**
   * Decider builder which just checks whether one of
   * the given Throwables matches the cause and restarts, otherwise escalates.
    * 如果给定的`Throwable`在`trapExit`集合中,则Restart,否则Escalate
   */
  def makeDecider(trapExit: immutable.Seq[Class[_ <: Throwable]]): Decider = {
    case x ⇒ if (trapExit exists (_ isInstance x)) Restart else Escalate
  }

  /**
   * Decider builder which just checks whether one of
   * the given Throwables matches the cause and restarts, otherwise escalates.
   */
  def makeDecider(trapExit: JIterable[Class[_ <: Throwable]]): Decider = makeDecider(immutableSeq(trapExit))

  /**
   * Decider builder for Iterables of cause-directive pairs, e.g. a map obtained
   * from configuration; will sort the pairs so that the most specific type is
   * checked before all its subtypes, allowing carving out subtrees of the
   * Throwable hierarchy.
   */
  def makeDecider(flat: Iterable[CauseDirective]): Decider = {
    val directives = sort(flat)

    { case x ⇒ directives collectFirst { case (c, d) if c isInstance x ⇒ d } getOrElse Escalate }
  }

  /**
   * Converts a Java Decider into a Scala Decider
   */
  def makeDecider(func: JDecider): Decider = { case x ⇒ func(x) }

  /**
   * Sort so that subtypes always precede their supertypes, but without
   * obeying any order between unrelated subtypes (insert sort).
   *
   * INTERNAL API
   */
  private[akka] def sort(in: Iterable[CauseDirective]): immutable.Seq[CauseDirective] =
    (new ArrayBuffer[CauseDirective](in.size) /: in) { (buf, ca) ⇒
      buf.indexWhere(_._1 isAssignableFrom ca._1) match {
        case -1 ⇒ buf append ca
        case x  ⇒ buf insert (x, ca)
      }
      buf
    }.to[immutable.IndexedSeq]

  private[akka] def withinTimeRangeOption(withinTimeRange: Duration): Option[Duration] =
    if (withinTimeRange.isFinite && withinTimeRange >= Duration.Zero) Some(withinTimeRange) else None

  private[akka] def maxNrOfRetriesOption(maxNrOfRetries: Int): Option[Int] =
    if (maxNrOfRetries < 0) None else Some(maxNrOfRetries)

  private[akka] val escalateDefault = (_: Any) ⇒ Escalate
}

/**
 * An Akka SupervisorStrategy is the policy to apply for crashing children.
  * 一个akka的监控策略,是应用到crash children的处理策略
 *
 * <b>IMPORTANT:</b>
 *
 * You should not normally need to create new subclasses, instead use the
  * 你不应该构建新的子类,应该使用已经存在的OneForOneStrategy或者AllForOneStrategy
 * existing [[akka.actor.OneForOneStrategy]] or [[akka.actor.AllForOneStrategy]],
 * but if you do, please read the docs of the methods below carefully, as
  * 但是如果你非要构建子类,请认真阅读下面各方法的说明,
 * incorrect implementations may lead to “blocked” actor systems (i.e.
  * 因为不正确的实现可能会导致整个ActorSystem的堵塞,比如永久的悬挂住actor
 * permanently suspended actors).
 */
abstract class SupervisorStrategy {

  import SupervisorStrategy._

  /**
   * Returns the Decider that is associated with this SupervisorStrategy.
    * 返回与监管策略相绑定的决策器
   * The Decider is invoked by the default implementation of `handleFailure`
    * 决策器会被`handleFailure`的默认实现调用,以获取需要执行的指令
   * to obtain the Directive to be applied.
   */
  def decider: Decider

  /**
   * This method is called after the child has been removed from the set of children.
    * 这个方法在child被从children中移除时调用
   * It does not need to do anything special. Exceptions thrown from this method
    * 不需要做任何特殊的处理
   * do NOT make the actor fail if this happens during termination.
    * 如果在终止时,这个方法抛出了异常,也不会导致actor失败
   */
  def handleChildTerminated(context: ActorContext, child: ActorRef, children: Iterable[ActorRef]): Unit

  /**
   * This method is called to act on the failure of a child: restart if the flag is true, stop otherwise.
    * 这个方法被调用在失败的actor身上执行
    * restart这个flag为ture时,进行重启,否则则进行stop
   */
  def processFailure(context: ActorContext, restart: Boolean, child: ActorRef, cause: Throwable, stats: ChildRestartStats, children: Iterable[ChildRestartStats]): Unit

  /**
   * This is the main entry point: in case of a child’s failure, this method
    * 这个方法是主要的入口 : 在子actor的失败情况下,这个方法必须尝试处理失败,可以通过
   * must try to handle the failure by resuming, restarting or stopping the
    * resuming、restarting、stopping这个child,并且返回true
   * child (and returning `true`), or it returns `false` to escalate the
    * 或者返回false,同时升级异常,也就是将exception向上层抛出
   * failure, which will lead to this actor re-throwing the exception which
   * caused the failure. The exception will not be wrapped.
    * 这个异常不会被包装,原封不动的抛出
   *
   * This method calls [[akka.actor.SupervisorStrategy#logFailure]], which will
    * 这个方法会调用`logFailure`,没有进行故障升级时,会打印这个失败信息
   * log the failure unless it is escalated. You can customize the logging by
    * 你可以自定义log,通过设置`loggingEnabled`为false,。。。。
   * setting [[akka.actor.SupervisorStrategy#loggingEnabled]] to `false` and
   * do the logging inside the `decider` or override the `logFailure` method.
   *
   * @param children is a lazy collection (a view)
   */
  def handleFailure(context: ActorContext, child: ActorRef, cause: Throwable, stats: ChildRestartStats, children: Iterable[ChildRestartStats]): Boolean = {
    val directive = decider.applyOrElse(cause, escalateDefault)
    directive match {
      case d @ Resume ⇒
        logFailure(context, child, cause, d)
        resumeChild(child, cause)
        true
      case d @ Restart ⇒
        logFailure(context, child, cause, d)
        processFailure(context, true, child, cause, stats, children)
        true
      case d @ Stop ⇒
        logFailure(context, child, cause, d)
        processFailure(context, false, child, cause, stats, children)
        true
      case d @ Escalate ⇒
        logFailure(context, child, cause, d)
        false
    }
  }

  /**
   * Logging of actor failures is done when this is `true`.
   */
  protected def loggingEnabled: Boolean = true

  /**
   * Default logging of actor failures when
   * [[akka.actor.SupervisorStrategy#loggingEnabled]] is `true`.
   * `Escalate` failures are not logged here, since they are supposed
   * to be handled at a level higher up in the hierarchy.
   * `Resume` failures are logged at `Warning` level.
   * `Stop` and `Restart` failures are logged at `Error` level.
   */
  def logFailure(context: ActorContext, child: ActorRef, cause: Throwable, decision: Directive): Unit =
    if (loggingEnabled) {
      val logMessage = cause match {
        case e: ActorInitializationException if e.getCause ne null ⇒ e.getCause.getMessage
        case e ⇒ e.getMessage
      }
      decision match {
        case Resume   ⇒ publish(context, Warning(child.path.toString, getClass, logMessage))
        case Escalate ⇒ // don't log here
        case _        ⇒ publish(context, Error(cause, child.path.toString, getClass, logMessage))
      }
    }

  // logging is not the main purpose, and if it fails there’s nothing we can do
  private def publish(context: ActorContext, logEvent: LogEvent): Unit =
    try context.system.eventStream.publish(logEvent) catch { case NonFatal(_) ⇒ }

  /**
   * Resume the previously failed child: <b>do never apply this to a child which
   * is not the currently failing child</b>. Suspend/resume needs to be done in
   * matching pairs, otherwise actors will wake up too soon or never at all.
   */
  final def resumeChild(child: ActorRef, cause: Throwable): Unit = child.asInstanceOf[InternalActorRef].resume(causedByFailure = cause)

  /**
   * Restart the given child, possibly suspending it first.
    * 重启给定的child,有可能先进行暂停操作
   *
   * <b>IMPORTANT:</b>
   *
   * If the child is the currently failing one, it will already have been
    * 如果child刚失败的,它应该已经被暂停了,因此`suspendFirst`必须为false
   * suspended, hence `suspendFirst` must be false. If the child is not the
   * currently failing one, then it did not request this treatment and is
    * 如果这个child不是刚失败的,
   * therefore not prepared to be resumed without prior suspend.
   */
  final def restartChild(child: ActorRef, cause: Throwable, suspendFirst: Boolean): Unit = {
    val c = child.asInstanceOf[InternalActorRef]
    if (suspendFirst) c.suspend()
    c.restart(cause)
  }

}

/**
 * Applies the fault handling `Directive` (Resume, Restart, Stop) specified in the `Decider`
  * 当有一个child失败时,将在`Decider`中指定的故障处理指令应用到所有的children中
 * to all children when one fails, as opposed to [[akka.actor.OneForOneStrategy]] that applies
  * 与`OneForOneStrategy`只应用到那个失败的child不同
 * it only to the child actor that failed.
 *
 * @param maxNrOfRetries the number of times a child actor is allowed to be restarted, negative value means no limit,
 *   if the limit is exceeded the child actor is stopped    child允许重启的次数,负数意味着不做限制,如果达到阈值,则stop
 * @param withinTimeRange duration of the time window for maxNrOfRetries, Duration.Inf means no window 窗口大小,Duration.Inf意味着没有窗口
 * @param decider mapping from Throwable to [[akka.actor.SupervisorStrategy.Directive]], you can also use a
 *   [[scala.collection.immutable.Seq]] of Throwables which maps the given Throwables to restarts, otherwise escalates.
 * @param loggingEnabled the strategy logs the failure if this is enabled (true), by default it is enabled
 */
case class AllForOneStrategy(
  maxNrOfRetries: Int = -1,
  withinTimeRange: Duration = Duration.Inf,
  override val loggingEnabled: Boolean = true)(val decider: SupervisorStrategy.Decider)
  extends SupervisorStrategy {

  import SupervisorStrategy._

  /**
   * Java API
   */
  def this(maxNrOfRetries: Int, withinTimeRange: Duration, decider: SupervisorStrategy.JDecider, loggingEnabled: Boolean) =
    this(maxNrOfRetries, withinTimeRange, loggingEnabled)(SupervisorStrategy.makeDecider(decider))

  /**
   * Java API
   */
  def this(maxNrOfRetries: Int, withinTimeRange: Duration, decider: SupervisorStrategy.JDecider) =
    this(maxNrOfRetries, withinTimeRange)(SupervisorStrategy.makeDecider(decider))

  /**
   * Java API
   */
  def this(maxNrOfRetries: Int, withinTimeRange: Duration, trapExit: JIterable[Class[_ <: Throwable]]) =
    this(maxNrOfRetries, withinTimeRange)(SupervisorStrategy.makeDecider(trapExit))

  /**
   * Java API: compatible with lambda expressions
   * This is an EXPERIMENTAL feature and is subject to change until it has received more real world testing.
   */
  def this(maxNrOfRetries: Int, withinTimeRange: Duration, decider: SupervisorStrategy.Decider) =
    this(maxNrOfRetries = maxNrOfRetries, withinTimeRange = withinTimeRange)(decider)

  /**
   * Java API: compatible with lambda expressions
   * This is an EXPERIMENTAL feature and is subject to change until it has received more real world testing.
   */
  def this(loggingEnabled: Boolean, decider: SupervisorStrategy.Decider) =
    this(loggingEnabled = loggingEnabled)(decider)

  /**
   * Java API: compatible with lambda expressions
   * This is an EXPERIMENTAL feature and is subject to change until it has received more real world testing.
   */
  def this(decider: SupervisorStrategy.Decider) =
    this()(decider)

  /*
   *  this is a performance optimization to avoid re-allocating the pairs upon
   *  every call to requestRestartPermission, assuming that strategies are shared
   *  across actors and thus this field does not take up much space
   */
  private val retriesWindow = (maxNrOfRetriesOption(maxNrOfRetries), withinTimeRangeOption(withinTimeRange).map(_.toMillis.toInt))

  def handleChildTerminated(context: ActorContext, child: ActorRef, children: Iterable[ActorRef]): Unit = ()

  def processFailure(context: ActorContext, restart: Boolean, child: ActorRef, cause: Throwable, stats: ChildRestartStats, children: Iterable[ChildRestartStats]): Unit = {
    if (children.nonEmpty) {
      if (restart && children.forall(_.requestRestartPermission(retriesWindow)))
        children foreach (crs ⇒ restartChild(crs.child, cause, suspendFirst = (crs.child != child)))
      else
        for (c ← children) context.stop(c.child)
    }
  }
}

/**
 * Applies the fault handling `Directive` (Resume, Restart, Stop) specified in the `Decider`
 * to the child actor that failed, as opposed to [[akka.actor.AllForOneStrategy]] that applies
 * it to all children.
 *
 * @param maxNrOfRetries the number of times a child actor is allowed to be restarted, negative value means no limit,
 *   if the limit is exceeded the child actor is stopped
 * @param withinTimeRange duration of the time window for maxNrOfRetries, Duration.Inf means no window
 * @param decider mapping from Throwable to [[akka.actor.SupervisorStrategy.Directive]], you can also use a
 *   [[scala.collection.immutable.Seq]] of Throwables which maps the given Throwables to restarts, otherwise escalates.
 * @param loggingEnabled the strategy logs the failure if this is enabled (true), by default it is enabled
 */
case class OneForOneStrategy(
  maxNrOfRetries: Int = -1,
  withinTimeRange: Duration = Duration.Inf,
  override val loggingEnabled: Boolean = true)(val decider: SupervisorStrategy.Decider)
  extends SupervisorStrategy {

  /**
   * Java API
   */
  def this(maxNrOfRetries: Int, withinTimeRange: Duration, decider: SupervisorStrategy.JDecider, loggingEnabled: Boolean) =
    this(maxNrOfRetries, withinTimeRange, loggingEnabled)(SupervisorStrategy.makeDecider(decider))

  /**
   * Java API
   */
  def this(maxNrOfRetries: Int, withinTimeRange: Duration, decider: SupervisorStrategy.JDecider) =
    this(maxNrOfRetries, withinTimeRange)(SupervisorStrategy.makeDecider(decider))

  /**
   * Java API
   */
  def this(maxNrOfRetries: Int, withinTimeRange: Duration, trapExit: JIterable[Class[_ <: Throwable]]) =
    this(maxNrOfRetries, withinTimeRange)(SupervisorStrategy.makeDecider(trapExit))

  /**
   * Java API: compatible with lambda expressions
   * This is an EXPERIMENTAL feature and is subject to change until it has received more real world testing.
   */
  def this(maxNrOfRetries: Int, withinTimeRange: Duration, decider: SupervisorStrategy.Decider) =
    this(maxNrOfRetries = maxNrOfRetries, withinTimeRange = withinTimeRange)(decider)

  /**
   * Java API: compatible with lambda expressions
   * This is an EXPERIMENTAL feature and is subject to change until it has received more real world testing.
   */
  def this(loggingEnabled: Boolean, decider: SupervisorStrategy.Decider) =
    this(loggingEnabled = loggingEnabled)(decider)

  /**
   * Java API: compatible with lambda expressions
   * This is an EXPERIMENTAL feature and is subject to change until it has received more real world testing.
   */
  def this(decider: SupervisorStrategy.Decider) =
    this()(decider)

  /*
   *  this is a performance optimization to avoid re-allocating the pairs upon
   *  every call to requestRestartPermission, assuming that strategies are shared
   *  across actors and thus this field does not take up much space
   */
  private val retriesWindow = (
    SupervisorStrategy.maxNrOfRetriesOption(maxNrOfRetries),
    SupervisorStrategy.withinTimeRangeOption(withinTimeRange).map(_.toMillis.toInt))

  def handleChildTerminated(context: ActorContext, child: ActorRef, children: Iterable[ActorRef]): Unit = ()

  def processFailure(context: ActorContext, restart: Boolean, child: ActorRef, cause: Throwable, stats: ChildRestartStats, children: Iterable[ChildRestartStats]): Unit = {
    if (restart && stats.requestRestartPermission(retriesWindow))
      restartChild(child, cause, suspendFirst = false)
    else
      context.stop(child) //TODO optimization to drop child here already?
  }
}

