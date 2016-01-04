/*
 * wazza-thor
 * https://github.com/Wazzaio/wazza-thor
 * Copyright (C) 2013-2015  Duarte Barbosa, João Vazão Vasques
 *
 *    This program is free software: you can redistribute it and/or modify
 *    it under the terms of the GNU General Public License as published by
 *    the Free Software Foundation, either version 3 of the License, or
 *    (at your option) any later version.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    GNU General Public License for more details.
 *
 *    You should have received a copy of the GNU General Public License
 *    along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package wazza.thor.messages

import java.util.Date

trait JobResult
case class WZSuccess extends JobResult
case class WZFailure(ex: Exception) extends JobResult
case class JobCompleted(jobName: String, status: JobResult)
case class CoreJobCompleted(
  companyName: String,
  applicationName: String,
  name: String,
  lower: Date,
  upper: Date,
  platforms: List[String],
  paymentSystems: List[Int]
) extends JobResult

