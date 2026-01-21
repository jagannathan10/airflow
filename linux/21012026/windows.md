      from datetime import datetime
      from airflow import DAG
      
      from agent_windows_unified import AgentWindowsUnifiedOperator
      
      with DAG(
          dag_id="windows_basic_job",
          start_date=datetime(2025, 1, 1),
          schedule_interval=None,
          catchup=False,
          tags=["windows", "agent"],
      ) as dag:
      
          run_powershell = AgentWindowsUnifiedOperator(
              task_id="run_ps_script",
              target_server="WINDOWS_HOST:18443",
              command=r'powershell.exe -NoProfile -ExecutionPolicy Bypass -File C:\scripts\create_test_file.ps1',
              agent_conn_id="agent_default",
      
              # Optional
              stream_stdout=False,     # stderr only (recommended)
              timeout_seconds=None,    # ⭐ NO timeout (multi-week jobs supported)
          )

✅ Example 2: Parallel Windows Jobs (Different task_id → parallel execution)

      from datetime import datetime
      from airflow import DAG
      from airflow.operators.empty import EmptyOperator
      
      from agent_windows_unified import AgentWindowsUnifiedOperator
      
      with DAG(
          dag_id="windows_parallel_jobs",
          start_date=datetime(2025, 1, 1),
          schedule_interval=None,
          catchup=False,
          tags=["windows", "parallel"],
      ) as dag:
      
          start = EmptyOperator(task_id="start")
      
          job1 = AgentWindowsUnifiedOperator(
              task_id="job_1",
              target_server="WINDOWS_HOST:18443",
              command=r'C:\tools\job1.bat',
          )
      
          job2 = AgentWindowsUnifiedOperator(
              task_id="job_2",
              target_server="WINDOWS_HOST:18443",
              command=r'C:\tools\job2.ps1',
          )
      
          job3 = AgentWindowsUnifiedOperator(
              task_id="job_3",
              target_server="WINDOWS_HOST:18443",
              command=r'C:\tools\job3.exe --mode batch',
              stream_stdout=True,   # enable stdout only if needed
          )

    start >> [job1, job2, job3]


✅ Example 3: Sequential Jobs (Same Host)

      with DAG(
          dag_id="windows_sequential_jobs",
          start_date=datetime(2025, 1, 1),
          schedule_interval=None,
          catchup=False,
      ) as dag:
      
          step1 = AgentWindowsUnifiedOperator(
              task_id="step1",
              target_server="WINDOWS_HOST:18443",
              command=r'C:\batch\step1.bat',
          )
      
          step2 = AgentWindowsUnifiedOperator(
              task_id="step2",
              target_server="WINDOWS_HOST:18443",
              command=r'C:\batch\step2.bat',
          )
      
          step3 = AgentWindowsUnifiedOperator(
              task_id="step3",
              target_server="WINDOWS_HOST:18443",
              command=r'C:\batch\step3.bat',
          )
      
          step1 >> step2 >> step3

Example 4: Long-Running Job (No Timeout)

      AgentWindowsUnifiedOperator(
          task_id="long_running_windows_job",
          target_server="WINDOWS_HOST:18443",
          command=r'powershell.exe -File C:\jobs\monthly_recon.ps1',
          timeout_seconds=None,    # ⭐ critical
      )

What does NOT kill the job
Event	Remote job
Scheduler restart	continues
Worker pod killed	continues
DAG reparse	continues
Task retried automatically	does NOT re-run
New trigger while running	returns already_running

Manual Cancel from Airflow UI (only way to kill job)

Use a separate cancel DAG:

from datetime import datetime
from airflow import DAG
from agent_windows_cancel import AgentWindowsCancelOperator

      with DAG(
          dag_id="windows_agent_cancel",
          start_date=datetime(2025, 1, 1),
          schedule_interval=None,
          catchup=False,
      ) as dag:
      
          cancel_job = AgentWindowsCancelOperator(
              task_id="cancel_remote_job",
              target_server="WINDOWS_HOST:18443",
              job_id="{{ dag_run.conf['job_id'] }}",
              agent_conn_id="agent_default",
          )
