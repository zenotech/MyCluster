

import mycluster

mycluster.create_submit('hybrid:hybrid.q',script_name='test.job',{'num_tasks': 2,
                                           'tasks_per_node':2,
                                           'my_script':'test.bsh',
                                           })

mycluster.submit('test.job')

mycluster.list()