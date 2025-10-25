from simpy_helpers import Entity


class FixedPoolEntity(Entity):
    """
    Entity for a simulation with a fixed-capacity 'simpy.Resource'.
    """

    def process(self):
        agent_pool = self.entity_args['agent_pool']
        kpis = self.entity_args['kpis']
        stats = self.entity_args['stats']
        patience = self.entity_args['patience']

        call_duration = self.attributes['call_duration']
        arrival_time = self.env.now

        with agent_pool.request() as req:
            patience_timeout = self.env.timeout(patience)
            result = yield req | patience_timeout

            wait_time = self.env.now - arrival_time

            if req in result:
                kpis.total_answered += 1
                kpis.wait_times.append(wait_time)
                if wait_time <= kpis.sla_threshold:
                    kpis.sla_met_count += 1

                stats.completed_entities += 1

                yield self.env.timeout(call_duration)
            else:
                kpis.total_abandoned += 1
