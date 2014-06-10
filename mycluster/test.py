

import mycluster

mycluster.create_submit('hybrid:hybrid.q',script_name='test.job',num_tasks=2,
                                                                   tasks_per_node=2,
                                                                   my_script='test.bsh',
                                                                   user_email='test@email.com',
                                                                   )
mycluster.submit('test.job')

for i in mycluster.job_list():
    print i, mycluster.get_job(i).status