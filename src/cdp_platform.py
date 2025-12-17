"""Intelligent Customer Data Platform

360° customer view, real-time segmentation, predictive analytics,
journey orchestration, GDPR/CCPA compliance.
"""

import asyncio
import logging
from typing import Dict, List, Any, Optional
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
import json
import hashlib

logger = logging.getLogger(__name__)

class CustomerSegment(Enum):
    HIGH_VALUE = "high_value"
    AT_RISK = "at_risk"
    ACTIVE = "active"
    DORMANT = "dormant"
    NEW = "new"

@dataclass
class Customer:
    id: str
    email: str
    name: str
    attributes: Dict[str, Any] = field(default_factory=dict)
    events: List[Dict[str, Any]] = field(default_factory=list)
    segments: List[CustomerSegment] = field(default_factory=list)
    lifetime_value: float = 0.0
    last_activity: Optional[datetime] = None
    created_at: datetime = field(default_factory=datetime.now)

class UnifiedProfileEngine:
    def __init__(self):
        self.profiles: Dict[str, Customer] = {}
        self.identity_graph: Dict[str, Set[str]] = {}
        
    def merge_identities(self, email: str, user_id: str, device_id: str):
        key = hashlib.md5(email.encode()).hexdigest()
        if key not in self.identity_graph:
            self.identity_graph[key] = set()
        self.identity_graph[key].update([email, user_id, device_id])
        
    def create_profile(self, customer: Customer) -> str:
        self.profiles[customer.id] = customer
        logger.info(f"Created profile for {customer.email}")
        return customer.id
        
    def get_360_view(self, customer_id: str) -> Dict[str, Any]:
        if customer_id not in self.profiles:
            return {}
        customer = self.profiles[customer_id]
        return {
            'id': customer.id,
            'email': customer.email,
            'name': customer.name,
            'lifetime_value': customer.lifetime_value,
            'segments': [s.value for s in customer.segments],
            'total_events': len(customer.events),
            'last_activity': customer.last_activity,
            'attributes': customer.attributes
        }

class RealtimeSegmentation:
    def __init__(self):
        self.segments: Dict[CustomerSegment, List[str]] = {s: [] for s in CustomerSegment}
        self.rules = {
            CustomerSegment.HIGH_VALUE: lambda c: c.lifetime_value > 10000,
            CustomerSegment.AT_RISK: lambda c: c.last_activity and (datetime.now() - c.last_activity).days > 30,
            CustomerSegment.DORMANT: lambda c: c.last_activity and (datetime.now() - c.last_activity).days > 90,
            CustomerSegment.NEW: lambda c: (datetime.now() - c.created_at).days < 30,
            CustomerSegment.ACTIVE: lambda c: c.last_activity and (datetime.now() - c.last_activity).days < 7
        }
        
    async def segment_customer(self, customer: Customer):
        customer.segments = []
        for segment, rule in self.rules.items():
            if rule(customer):
                customer.segments.append(segment)
                if customer.id not in self.segments[segment]:
                    self.segments[segment].append(customer.id)
        logger.debug(f"Customer {customer.id} segmented: {[s.value for s in customer.segments]}")

class PredictiveAnalytics:
    def __init__(self):
        self.predictions: Dict[str, Dict[str, float]] = {}
        
    async def predict_churn(self, customer: Customer) -> float:
        score = 0.0
        if customer.last_activity:
            days_inactive = (datetime.now() - customer.last_activity).days
            score = min(1.0, days_inactive / 90.0)
        self.predictions[customer.id] = {'churn_risk': score}
        return score
        
    async def predict_ltv(self, customer: Customer) -> float:
        base_value = customer.lifetime_value
        activity_multiplier = 1.0 + (len(customer.events) / 100.0)
        predicted_ltv = base_value * activity_multiplier * 1.2
        if customer.id not in self.predictions:
            self.predictions[customer.id] = {}
        self.predictions[customer.id]['predicted_ltv'] = predicted_ltv
        return predicted_ltv

class JourneyOrchestrator:
    def __init__(self):
        self.journeys: Dict[str, List[Dict[str, Any]]] = {}
        self.active_journeys: Dict[str, str] = {}
        
    def define_journey(self, name: str, stages: List[Dict[str, Any]]):
        self.journeys[name] = stages
        logger.info(f"Defined journey: {name} with {len(stages)} stages")
        
    async def enroll_customer(self, customer_id: str, journey_name: str):
        if journey_name in self.journeys:
            self.active_journeys[customer_id] = journey_name
            logger.info(f"Enrolled customer {customer_id} in {journey_name}")
            
    async def advance_journey(self, customer_id: str, stage: int):
        if customer_id in self.active_journeys:
            journey = self.active_journeys[customer_id]
            logger.debug(f"Customer {customer_id} advanced to stage {stage} in {journey}")

class PrivacyCompliance:
    def __init__(self):
        self.consent_records: Dict[str, Dict[str, Any]] = {}
        self.deletion_requests: List[Dict[str, Any]] = []
        
    async def record_consent(self, customer_id: str, purpose: str, granted: bool):
        if customer_id not in self.consent_records:
            self.consent_records[customer_id] = {}
        self.consent_records[customer_id][purpose] = {
            'granted': granted,
            'timestamp': datetime.now()
        }
        logger.info(f"Consent recorded for {customer_id}: {purpose} = {granted}")
        
    async def process_deletion_request(self, customer_id: str, profiles: Dict[str, Customer]):
        if customer_id in profiles:
            del profiles[customer_id]
            self.deletion_requests.append({
                'customer_id': customer_id,
                'processed_at': datetime.now()
            })
            logger.info(f"Deleted data for customer {customer_id}")

class IntelligentCDP:
    def __init__(self):
        self.profile_engine = UnifiedProfileEngine()
        self.segmentation = RealtimeSegmentation()
        self.predictive = PredictiveAnalytics()
        self.journey = JourneyOrchestrator()
        self.privacy = PrivacyCompliance()
        
    async def demo(self):
        logger.info("\n" + "="*60)
        logger.info("INTELLIGENT CUSTOMER DATA PLATFORM")
        logger.info("="*60)
        
        # Create customers
        for i in range(100):
            customer = Customer(
                id=f"cust-{i}",
                email=f"customer{i}@example.com",
                name=f"Customer {i}",
                lifetime_value=1000 + i * 100,
                last_activity=datetime.now() - timedelta(days=i % 60)
            )
            self.profile_engine.create_profile(customer)
            await self.segmentation.segment_customer(customer)
            await self.predictive.predict_churn(customer)
            await self.predictive.predict_ltv(customer)
            
        # Journey orchestration
        self.journey.define_journey("onboarding", [
            {'stage': 'welcome', 'action': 'send_email'},
            {'stage': 'tutorial', 'action': 'show_guide'},
            {'stage': 'first_purchase', 'action': 'offer_discount'}
        ])
        
        # Privacy compliance
        await self.privacy.record_consent("cust-0", "marketing", True)
        
        logger.info(f"\nCustomers: {len(self.profile_engine.profiles)}")
        logger.info(f"Segments:")
        for segment, customers in self.segmentation.segments.items():
            logger.info(f"  {segment.value}: {len(customers)}")
        logger.info(f"Predictions: {len(self.predictive.predictions)}")
        logger.info(f"Consent Records: {len(self.privacy.consent_records)}")
        logger.info("\n" + "="*60)

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
    cdp = IntelligentCDP()
    asyncio.run(cdp.demo())
