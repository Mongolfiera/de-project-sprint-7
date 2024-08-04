# Проект 7-го спринта

### Описание
Репозиторий предназначен для сдачи проекта 7-го спринта

### Как работать с репозиторием
1. В вашем GitHub-аккаунте автоматически создастся репозиторий `de-project-sprint-7` после того, как вы привяжете свой GitHub-аккаунт на Платформе.
2. Скопируйте репозиторий на свой компьютер. В качестве пароля укажите ваш `Access Token`, который нужно получить на странице [Personal Access Tokens](https://github.com/settings/tokens)):
	* `git clone https://github.com/{{ username }}/de-project-sprint-7.git`
3. Перейдите в директорию с проектом: 
	* `cd de-project-sprint-7`
4. Выполните проект и сохраните получившийся код в локальном репозитории:
	* `git add .`
	* `git commit -m 'my best commit'`
5. Обновите репозиторий в вашем GutHub-аккаунте:
	* `git push origin main`

### Структура репозитория
Вложенные файлы в репозиторий будут использоваться для проверки и предоставления обратной связи по проекту. Поэтому постарайтесь публиковать ваше решение согласно установленной структуре — так будет проще соотнести задания с решениями.

Внутри `src` расположены две папки:
- `/src/dags`;
- `/src/sql`.


# Описание витрин
## 1. Витрина в разрезе пользователей

- `user_id` — идентификатор пользователя.
- `act_city` — актуальный адрес. Это город, из которого было отправлено последнее сообщение.
- `home_city` — домашний адрес. Это последний город, в котором пользователь был дольше 27 дней.
*Домашний адрес определяется как последний город, в котором пользователь был больше 27 дней до изменения города или до последней даты рассматриваемого периода. Если такой город не находится, будет взят первый город.*
- `travel_count` — количество посещённых городов. Если пользователь побывал в каком-то городе повторно, то это считается за отдельное посещение.
- `travel_array` — список городов в порядке посещения.
- `local_time` - местное время. 


## 2. Витрина в разрезе геозон
- `month` — месяц расчёта.
- `week` — неделя расчёта.
- `zone_id` — идентификатор зоны (города).
- `week_message` — количество сообщений за неделю.
- `week_reaction` — количество реакций за неделю.
- `week_subscription` — количество подписок за неделю.
- `week_user` — количество регистраций за неделю.
- `month_message` — количество сообщений за месяц.
- `month_reaction` — количество реакций за месяц.
- `month_subscription` — количество подписок за месяц.
- `month_user` — количество регистраций за месяц.


## 3. Витрина для рекомендации друзей
*Если пользователи подписаны на один канал, ранее никогда не переписывались и расстояние между ними не превышает 1 км, то им обоим будет предложено добавить другого в друзья.*
- `user_left` — первый пользователь.
- `user_right` — второй пользователь.
- `processed_dttm` — дата расчёта витрины.
*в качестве даты расчета витрины берется последняя дата рассматриваемого периода*
- `zone_id` — идентификатор зоны (города).
- `local_time` — локальное время (часовой пояс).
