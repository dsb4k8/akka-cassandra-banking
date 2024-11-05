package com.dsb4k8.bank.actors

import akka.actor.typed.{ActorRef, Behavior}
import akka.persistence.typed.PersistenceId
import akka.persistence.typed.scaladsl.{Effect, EventSourcedBehavior}

// a single account
// receives messages via HTTP and will store events to Cassandra
// Cassandra is document-oriented meaning it is less of a database and more of a data-store.
// Instead of storing only the most recent version of the data, cassandra keeps track of the entire evolution in a log-
// table type storage structure (sstables)
/* Event sourcing like this has several benefits
* Fault Tolerance: If a server node goes down, the path to the most recent data presists and can be retreive on reboot
* Auditing: If there are suspicious transactions happen, they can be investigated and acton can be taken by the business
*/
class PersistentBankAccount {
  //Needs to handle the following:


  //commands = messages
  sealed trait Command
  case class CreateBankAccount(user: String, currency: String, initialBalance: Double, replyTo: ActorRef[Response]) extends Command
  case class UpdateBalance(id: String, currency: String, amount: Double /*can be negative*/, replyTo: ActorRef[Response]) extends Command
  case class GetBankAccount(id: String, replyTo: ActorRef[Response]) extends Command



  // events = to persist to Cassandra
  trait Event
  case class BankAccountCreated(bankAccount: BankAccount) extends Event
  case class BalanceUpdated(amount: Double) extends Event


  //state
  case class BankAccount(id: String, user: String, currency: String, balance:Double)

  //responses
  sealed trait Response
  case class BankAccountCreatedResponse(id: String) extends Response
  case class BankAccountBalanceUpdatedResponse(maybeAccount: Option[BankAccount]) extends Response
  case class GetBankAccountResponse(maybeBankAccount: Option[BankAccount]) extends Response


//  Presistent actors are defined according to the following
  /*
  * command handler = message handler => persist an event
  * event handler => updates state
  * the state is then used as the base for future computation*/

  val commandHandler: (BankAccount, Command) => Effect[Event, BankAccount] = (state, command) =>
    command match {
      case CreateBankAccount(user, currency, initialBalance, bank) => val id = state.id
        /*for command handler actor:
        - the bank creates me
        - Bank send CreateBankAccount Command
        - I persist (read instructions, then write them to my node) the BankAccountCreated
        - I update my state
        - then I reply back to bank with BankAccountCreatedResponse
        - then the bank later surfaces the response to the HTTP server.
         */
        Effect
          .persist(BankAccountCreated(BankAccount(id, user, currency, initialBalance)))
          .thenReply(bank)(_ => BankAccountCreatedResponse(id))

      case UpdateBalance(_,_,amount, bank) =>
        val newBalance = state.balance + amount
        if (newBalance < 0) //overdraft; illegal
          Effect.reply(bank)(BankAccountBalanceUpdatedResponse(None))
        else
          Effect.persist(BalanceUpdated(amount))
          .thenReply(bank)(newState => BankAccountBalanceUpdatedResponse(Some(newState)))
      case GetBankAccount(_, bank) =>
        Effect.reply(bank)(GetBankAccountResponse(Some(state)))
    }
  val eventHandler: (BankAccount, Event) => BankAccount = (state, event) =>
    event match {
      case BankAccountCreated(bankAccount) => bankAccount
      case BalanceUpdated(amount) => state.copy(balance = state.balance + amount)
    }

  def apply(id: String): Behavior[Command] =
    EventSourcedBehavior[Command, Event, BankAccount] (
      persistenceId = PersistenceId.ofUniqueId(id),
      emptyState = BankAccount(id, "", "", 0.0), //unused but important for persistence
      commandHandler = commandHandler,
      eventHandler = eventHandler
    )

}
