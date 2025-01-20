from sqlalchemy import Column, Integer, DateTime, create_engine, String, NullPool
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from datetime import datetime, timedelta

Base = declarative_base()


class Customer(Base):
    __tablename__ = 'customers'
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(100), nullable=False)
    mobile_number = Column(String(15), nullable=False, unique=True)  # Mobile number column
    email_id = Column(String(100),nullable=False,unique=True)
    event_name = Column(String(100), nullable=False)
    event_types = Column(String(500), nullable=False)
    contract_address = Column(String(500), nullable=False)
    token_id = Column(String(50), nullable=False)
    api_key = Column(String(500), nullable=False)
    frequency = Column(String(50), nullable=False)
    last_sent = Column(DateTime(timezone=True), nullable=True)  # PostgreSQL supports timezone-aware datetime
    next_due = Column(DateTime(timezone=True), nullable=True)  # Timezone-aware datetime


class DatabaseHandler:
    def __init__(self, database_url):
        """
        Initialize the database engine and session maker.
        """
        self.engine = create_engine(database_url, poolclass=NullPool)
        self.Session = sessionmaker(bind=self.engine)
        self.Session.configure(bind=self.engine)
        Base.metadata.create_all(self.engine)

    def add_customer(self, name, mobile_number, email_id, event_name, event_types, contract_address,
                     token_id, api_key, frequency, last_sent, next_due):
        """
        Adds a new customer to the database.
        """
        session = self.Session()
        try:
            new_customer = Customer(
                name=name,
                mobile_number=mobile_number,
                email_id = email_id,
                event_name=event_name,
                event_types=event_types,
                contract_address=contract_address,
                token_id=token_id,
                api_key=api_key,
                frequency=frequency,
                last_sent=last_sent,
                next_due=next_due,
            )
            session.add(new_customer)
            session.commit()
        except Exception as e:
            session.rollback()
            print(f"Error adding customer: {e}")
        finally:
            session.close()

    def fetch_customers(self):
        """
        Fetches all customers from the database.
        """
        session = self.Session()
        try:
            return session.query(Customer).all()
        except Exception as e:
            print(f"Error fetching customers: {e}")
            return []
        finally:
            session.close()

    def fetch_due_customers(self):
        """
        Fetch customers whose next_due is less than or equal to the current datetime.
        """
        session = self.Session()
        try:
            now = datetime.now()
            customers = session.query(
                Customer.id,
                Customer.mobile_number,
                Customer.email_id,
                Customer.event_name,
                Customer.event_types,
                Customer.contract_address,
                Customer.token_id,
                Customer.api_key,
                Customer.frequency,
                Customer.last_sent,
            ).filter(Customer.next_due <= now).all()

            return customers
        except Exception as e:
            print(f"Error fetching due customers: {e}")
            return []
        finally:
            session.close()

    def update_customer_schedule(self, customer_id, new_last_sent, new_next_due):
        """
        Updates the last_sent and next_due fields for a customer.
        """
        session = self.Session()
        try:
            customer = session.query(Customer).filter_by(id=customer_id).first()
            if customer:
                customer.last_sent = new_last_sent
                customer.next_due = new_next_due
                session.commit()
                print(f"Updated customer ID {customer_id} successfully.")
            else:
                print(f"Customer with ID {customer_id} not found.")
        except Exception as e:
            session.rollback()
            print(f"Error updating customer ID {customer_id}: {e}")
        finally:
            session.close()
