from segmentation_tasks.tasks import run_segmentation, run_cohort_analysis, run_timeline

def launch_initial_tasks():
    run_segmentation.delay()
    run_cohort_analysis.delay()
    run_timeline.delay()
    
if __name__ == "__main__":
    launch_initial_tasks()