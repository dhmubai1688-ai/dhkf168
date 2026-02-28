async def _export_yesterday_data_concurrent(
    chat_id: int, target_date: date, from_monthly: bool = False
) -> bool:
    """并发导出数据，成功一次就推送"""
    from main import export_and_push_csv
    
    source = "月度表" if from_monthly else "日常表"
    
    # 使用锁确保只有一个任务能执行推送
    push_lock = asyncio.Lock()
    push_completed = False
    
    async def task_wrapper(attempt: int) -> bool:
        nonlocal push_completed
        
        file_name = f"dual_shift_backup_{chat_id}_{target_date.strftime('%Y%m%d')}.csv"
        
        try:
            # 执行导出，但还不确定是否推送
            result = await export_and_push_csv(
                chat_id=chat_id,
                target_date=target_date,
                file_name=file_name,
                is_daily_reset=True,
                from_monthly_table=True,
                push_file=False,  # 先不推送，只生成数据
            )
            
            if result:
                # 数据生成成功，现在决定是否推送
                should_push = False
                async with push_lock:
                    if not push_completed:
                        should_push = True
                        push_completed = True
                
                if should_push:
                    # 需要推送：重新调用但只推送（可以优化为直接使用已生成的文件）
                    await export_and_push_csv(
                        chat_id=chat_id,
                        target_date=target_date,
                        file_name=file_name,
                        is_daily_reset=True,
                        from_monthly_table=True,
                        push_file=True,
                    )
                    logger.info(f"✅ [数据导出] 群组{chat_id} 第{attempt+1}次尝试成功，已推送")
                else:
                    logger.info(f"✅ [数据导出] 群组{chat_id} 第{attempt+1}次尝试成功，已跳过")
                
                return True
            return False
            
        except Exception as e:
            logger.warning(f"⚠️ [数据导出] 第{attempt+1}次尝试失败: {e}")
            return False
    
    tasks = [asyncio.create_task(task_wrapper(i)) for i in range(3)]
    results = await asyncio.gather(*tasks)
    success_count = sum(1 for r in results if r is True)
    
    if push_completed:
        logger.info(f"📊 [数据导出] 群组{chat_id} 共 {success_count} 次成功，已推送1次")
        return True
    else:
        logger.error(f"❌ [数据导出] 群组{chat_id} 所有3次尝试均失败")
        return False
