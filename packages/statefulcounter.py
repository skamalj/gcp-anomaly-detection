from apache_beam import DoFn
import logging


class StatefulCounterDoFn(DoFn):
    from apache_beam.transforms.userstate import TimerSpec, ReadModifyWriteStateSpec, on_timer, TimeDomain
    from apache_beam.coders.coders import VarIntCoder
    from apache_beam import DoFn
    

    COUNTER_STATE_SPEC = ReadModifyWriteStateSpec('event_count', VarIntCoder())
    TIMER = TimerSpec('timer', TimeDomain.REAL_TIME)

    def __init__(self, limit_count):
        self.limit_count = limit_count

    def process(self,
                element_pair,
                counter_state=DoFn.StateParam(COUNTER_STATE_SPEC),
                timer=DoFn.TimerParam(TIMER)):
        from apache_beam.utils.timestamp import Timestamp

        current_value = counter_state.read() or 0
        if current_value >= 0:
            if current_value + 1 >= self.limit_count:
                logging.info("Resetting counter for {}".format(element_pair))
                counter_state.write(-1)            
                new_time = Timestamp(seconds=Timestamp.now().seconds() + 300)
                timer.set(new_time)
                yield element_pair
            else:
                counter_state.write(current_value + 1)
                return

    @on_timer(TIMER)
    def expiry_callback(self, counter_state=DoFn.StateParam(COUNTER_STATE_SPEC)):
        counter_state.clear()
        logging.info("State after timer reset is {}".format(counter_state.read()))

