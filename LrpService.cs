using BozicIO.Insurance.Api.Controllers.Alert;
using BozicIO.Insurance.Api.Models.Lrp.Analysis;
using BozicIO.Insurance.Api.Models.Lrp.Analysis.PriceEvolution;
using BozicIO.Insurance.Api.Models.Lrp.Analysis.PriceFutures;
using BozicIO.Insurance.Api.Models.Lrp.Analysis.PriceTrends;
using BozicIO.Insurance.Api.Models.Lrp.Endorsement;
using BozicIO.Insurance.Api.Models.Tag;
using BozicIO.Insurance.Api.Models.Udm;
using BozicIO.Insurance.Api.Models.Udm.Value.TimeSeries;
using BozicIO.Insurance.Models;
using BozicIO.Insurance.Models.Alert.Lrp;
using BozicIO.Insurance.Models.Extensions;
using BozicIO.Insurance.Models.Lrp;
using BozicIO.Insurance.Models.Tag;
using BozicIO.Library.Math;
using BozicIO.RiskPremium;
using BozicIO.RiskPremium.Lrp;
using MoreLinq;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using TypeCode = BozicIO.RiskPremium.TypeCode;

namespace BozicIO.Insurance.Api.Models.Lrp
{
	public class LrpService : ILrpService
	{
		private IUdmService UdmService { get; }
		private ILrpEndorsementRepository LrpEndorsementRepository { get; }
		private IValueTimeSeriesRepository<ValueDaily> ValueDailyRepository { get; }
		private IValueTimeSeriesRepository<ValueMonthly> ValueMonthlyRepository { get; }
		private TagService TagService { get; }

		private const int OptionTypeId = 2;

		private static Dictionary<TypeCode.LrpEnum, decimal> priceAdjustmentFactor = new Dictionary<TypeCode.LrpEnum, decimal>()
		{
			{ TypeCode.LrpEnum.SteersWeight1, 1.1M },
			{ TypeCode.LrpEnum.SteersWeight2, 1M },
			{ TypeCode.LrpEnum.HeifersWeight1, 1M },
			{ TypeCode.LrpEnum.HeifersWeight2, 0.9M },
			{ TypeCode.LrpEnum.BrahmanWeight1,  1M },
			{ TypeCode.LrpEnum.BrahmanWeight2, 0.9M },
			{ TypeCode.LrpEnum.DairyWeight1, 0.5M },
			{ TypeCode.LrpEnum.DairyWeight2, 0.5M },
			{ TypeCode.LrpEnum.UnbornSteersHeifers, 1.05M },
			{ TypeCode.LrpEnum.UnbornBrahman, 1M },
			{ TypeCode.LrpEnum.UnbornDairy, 0.5M },
			{ TypeCode.LrpEnum.SteersHeifers, 1M }
		};
		public InsurancePlan InsurancePlan => InsurancePlan.Lrp;

		public LrpService(
			IUdmService udmService,
			ILrpEndorsementRepository lrpEndorsementRepository,
			IValueTimeSeriesRepository<ValueDaily> valueDailyRepository,
			IValueTimeSeriesRepository<ValueMonthly> valueMonthlyRepository,
			TagService tagService
		)
		{
			UdmService = udmService;
			LrpEndorsementRepository = lrpEndorsementRepository;
			ValueDailyRepository = valueDailyRepository;
			ValueMonthlyRepository = valueMonthlyRepository;
			TagService = tagService;
		}

		public async Task<(DateTime salesEffectiveDate, IEnumerable<CommodityOption> commodities)> GetSalesSettingsAsync(DateTime? salesEffectiveDate = null, CommodityCode? commodity = null, TypeCode.LrpEnum? typeCode = null, int? endorsementLenghtCount = null)
		{
			var latestSalesEffectiveDate = await UdmService.GetLrpSalesDate(salesEffectiveDate);
			var reinsuranceYear = latestSalesEffectiveDate.GetReinsuranceYear();
			var lrpRates = await GetLrpRateAsync(latestSalesEffectiveDate);

			if (commodity.HasValue)
				lrpRates = lrpRates.Where(x => int.Parse(x.CommodityCode) == (int)commodity.Value);

			if (typeCode.HasValue)
				lrpRates = lrpRates.Where(x => int.Parse(x.TypeCode) == (int)typeCode.Value);

			if (endorsementLenghtCount.HasValue)
				lrpRates = lrpRates.Where(x => x.EndorsementLengthCount == endorsementLenghtCount.Value);

			var commodities = GetSalesSettings(latestSalesEffectiveDate, lrpRates.ToList());

			return (latestSalesEffectiveDate, commodities);
		}

		public IEnumerable<CommodityOption> GetSalesSettings(DateTime salesEffectiveDate, IEnumerable<Value_Special_ADM_A00630_LrpRate> lrpRates)
		{
			return lrpRates
				.GroupBy(lrpRate => lrpRate.CommodityCode)
				.Select(
					group =>
					{
						return new CommodityOption()
						{
							Commodity = (CommodityCode)int.Parse(group.Key),
							Types = group
								.GroupBy(lrpRate => lrpRate.TypeCode)
								.OrderBy(typegroup => typegroup.Key)
								.Select(
									typeGroup =>
									{
										return new CommodityType()
										{
											Type = (TypeCode.LrpEnum)int.Parse(typeGroup.Key),
											Periods = typeGroup
												.GroupBy(lrpRate => (int)lrpRate.EndorsementLengthCount)
												.OrderBy(periodGroup => periodGroup.Key)
												.Select(
													periodGroup =>
													{
														var rank = 1;
														return new EndorsementPeriod()
														{
															Length = periodGroup.Key,
															EndDate = salesEffectiveDate.AddDays(periodGroup.Key * 7),
															CoveragePercentages = periodGroup
																.Where(x => x.LivestockCoverageLevelPercent.HasValue)
																.OrderByDescending(x => x.LivestockCoverageLevelPercent)
																.Select(
																lrpRate => new CoveragePercentage()
																{
																	Rank = rank++,
																	Value = lrpRate.LivestockCoverageLevelPercent.Value,
																	CoveragePrice = lrpRate.CoveragePrice
																}
															)
														};
													}
												)
										};
									}
								)
						};
					}
				);
		}

		public Task<IEnumerable<LrpQuote>> GetQuotesOverall(LrpCalculationParameters parameters)
			=> GetQuotesAsync<LrpQuote>(
				parameters,
				GetLrpRateAsync(parameters.SalesEffectiveDate, parameters.Commodity, parameters.TypeCode)
			);

		public Task<IEnumerable<LrpQuote>> GetQuotesForEndorsementLength(LrpCalculationParameters parameters)
			=> GetQuotesAsync<LrpQuote>(
				parameters,
				GetRateForEndorsementLengthAsync(parameters.SalesEffectiveDate, parameters.Commodity, parameters.TypeCode, parameters.EndorsementLength)
			);

		public Task<IEnumerable<LrpQuote>> GetQuotesForCoverageRank(LrpCalculationParameters parameters)
			=> GetQuotesForCoverageRank<LrpQuote>(parameters);

		private Task<IEnumerable<T>> GetQuotesForCoverageRank<T>(LrpCalculationParameters parameters)
			where T : LrpQuote, new()
			=> GetQuotesAsync<T>(
				parameters,
				GetLrpRateForCoverageRankAsync(parameters.SalesEffectiveDate, parameters.Commodity, parameters.TypeCode, parameters.CoveragePercentageRank)
			);

		private async Task<IEnumerable<T>> GetQuotesAsync<T>(LrpCalculationParameters parameters, Task<IEnumerable<Value_Special_ADM_A00630_LrpRate>> ratesTask)
			where T : LrpQuote, new()
		{
			var subsidyPercentTask = UdmService.GetLrpSubsidyPercentAsync(parameters.SalesEffectiveDate.GetReinsuranceYear(), InsurancePlanCodes.Lrp);
			var futuresExpirationDatesTask = UdmService.GetOptionsExpirationDates(parameters.Commodity.OptionsProduct());

			var tasks = (await ratesTask)
				.Select(
					async rate =>
					{
						var coverageRate = CalculateQuote<T>(parameters, rate, await subsidyPercentTask);
						await CalculateFuturesAsync(coverageRate, parameters, await futuresExpirationDatesTask);
						return coverageRate;
					}
				);

			var quotes = await Task.WhenAll(tasks);
			await SetActualPriceAsync(
				parameters.Commodity,
				parameters.TypeCode,
				quotes,
				x => x.SalesEffectiveDate,
				x => x.EndDate,
				(x, value, date) => { x.ActualPriceCwt = value; x.ReleaseDate = date; }
			);

			return quotes;
		}

		public async Task<LrpHistoricViewModel> GetHistoricCoverage(LrpCalculationParameters parameters)
		{
			var historicViewModel = new LrpHistoricViewModel();

			var salesEffectiveDate = GetSalesEffectiveDate(parameters.SalesEffectiveDate);
			var subsidyPercent = await UdmService.GetLrpSubsidyPercentAsync(salesEffectiveDate.GetReinsuranceYear(), InsurancePlanCodes.Lrp);

			var potentialSalesEffectiveDates = GetHistoricSalesEffectiveDates(salesEffectiveDate, numberOfDays: 7);
			var potentialDailyRates = await GetRateForEndorsementLengthAsync(potentialSalesEffectiveDates, parameters.Commodity, parameters.TypeCode, parameters.EndorsementLength);

			var actualSalesEffectiveDates = GetActualSalesEffectiveDates(salesEffectiveDate, potentialDailyRates.Select(x => x.SalesEffectiveDate));
			var actualDailyRates = potentialDailyRates
				.Join(
					actualSalesEffectiveDates,
					dailyRate => dailyRate.SalesEffectiveDate,
					actualSalesEffectiveDate => actualSalesEffectiveDate,
					(dailyRate, actualSalesEffectiveDate) => dailyRate
				)
				.ToList();

			var actualPrices = await GetActualPrices(
				actualSalesEffectiveDates
					.Select(x => x.GetReinsuranceYear())
					.Distinct(),
				parameters.Commodity,
				parameters.TypeCode,
				actualDailyRates
					.Where(x => x.EndDate.HasValue)
					.Select(x => x.EndDate.Value)
					.ToList()
			);

			foreach (var date in actualSalesEffectiveDates)
			{
				var dailyRate = actualDailyRates
					.Where(x => x.SalesEffectiveDate == date)
					.OrderByDescending(x => x.LivestockCoverageLevelPercent)
					.Take(10)
					.ElementAtOrDefault(parameters.CoveragePercentageRank - 1);

				if (dailyRate == null)
					continue;

				var quote = CalculateQuote<LrpQuote>(
					parameters,
					dailyRate,
					subsidyPercent
				);

				quote.ActualPriceCwt = actualPrices.FirstOrDefault(
					x => x.EndDate == quote.EndDate && x.StateCode == dailyRate.StateCode && x.PracticeCode == dailyRate.PracticeCode
				)?.ActualEndingValueAmount;

				historicViewModel.HistoricData.Add(quote);
			}

			historicViewModel.HistoricData = historicViewModel.HistoricData.OrderByDescending(x => x.SalesEffectiveDate).ThenByDescending(x => x.EndDate).ToList();
			historicViewModel.HistoricDataAvg = CalculateHistoricAvg(historicViewModel.HistoricData.Where(x => x.SalesEffectiveDate.Year < salesEffectiveDate.Year));
			historicViewModel.HistoricDataAvgYear5 = CalculateHistoricAvg(historicViewModel.HistoricData.Where(x => x.SalesEffectiveDate.Year < salesEffectiveDate.Year).Take(5));

			return historicViewModel;
		}

		public async Task<LrpEndorsementModel> CreateModelAsync(int operationId, LrpEndorsementDefinition definition)
		{
			var endorsement = new LrpEndorsementModel()
			{
				OperationId = operationId,
				StateCode = definition.StateCode,
				CountyCode = definition.CountyCode,
				ReinsuranceYear = definition.CalculationParameters.SalesEffectiveDate.GetReinsuranceYear(),
				CalendarYear = definition.CalculationParameters.SalesEffectiveDate.Year,
				Commodity = definition.CalculationParameters.Commodity,
				TypeCode = definition.CalculationParameters.TypeCode,
				SalesEffectiveDate = definition.CalculationParameters.SalesEffectiveDate,
				NumberOfHead = definition.CalculationParameters.NumberOfHead,
				Share = definition.CalculationParameters.InsuredSharePercent,
				ConservationCompliancePercent = definition.CalculationParameters.ConservationCompliance,
				TargetWeight = definition.CalculationParameters.TargetWeight,
				CoveragePercentageRank = definition.CalculationParameters.CoveragePercentageRank,
				IsBeginner = definition.CalculationParameters.IsBeginner,
				EndorsementLength = definition.CalculationParameters.EndorsementLength,
				Status = definition.Status,
				BrokerageNotes = definition.BrokerageNotes,
				PenIdentifier = definition.PenIdentifier,
				Tags = definition.Tags
			};

			await UpdateModelAsync(endorsement);

			return endorsement;
		}

		private async Task<LrpEndorsementModel> UpdateModelAsync(LrpEndorsementModel endorsement)
		{
			var quote = (
				await GetQuotesForCoverageRank(
					new LrpCalculationParameters()
					{
						Commodity = endorsement.Commodity,
						TypeCode = endorsement.TypeCode,
						SalesEffectiveDate = endorsement.SalesEffectiveDate,
						NumberOfHead = endorsement.NumberOfHead,
						InsuredSharePercent = endorsement.Share,
						ConservationCompliance = endorsement.ConservationCompliancePercent,
						TargetWeight = endorsement.TargetWeight,
						CoveragePercentageRank = endorsement.CoveragePercentageRank,
						IsBeginner = endorsement.IsBeginner,
						EndorsementLength = endorsement.EndorsementLength
					}
				)
			)
				.FirstOrDefault(x => x.EndorsementLength == endorsement.EndorsementLength);

			if (quote == null)
			{
				endorsement.FinalUpdate = true;
			}
			else
			{
				endorsement.CoveragePercent = quote.CoveragePercent;
				endorsement.CoveragePriceCwt = quote.CoveragePriceCwt;
				endorsement.ActualPriceCwt = quote.ActualPriceCwt;
				endorsement.EndDate = quote.EndDate;
				endorsement.ExpectedPriceCwt = quote.ExpectedPriceCwt;
				endorsement.Liability = quote.Liability;
				endorsement.NetGuarantee = quote.NetGuarantee;
				endorsement.PracticeCode = quote.PracticeCode;
				endorsement.Premium = quote.TotalPremium;
				endorsement.PremiumProducer = quote.ProducerPremium;
				endorsement.SubsidyAmount = quote.SubsidyAmount;
				endorsement.ConservationComplianceSubsidyReduction = quote.ConservationComplianceSubsidyReduction;

				if (endorsement.ActualPriceCwt.Value > 0)
					endorsement.ActualPriceDate = quote.ReleaseDate;
			}

			return endorsement;
		}

		public async Task<LrpEndorsementModel> CreateAsync(int operationId, LrpEndorsementDefinition definition)
			=> (await CreateAsync(operationId, new[] { definition })).FirstOrDefault();

		public async Task<IEnumerable<LrpEndorsementModel>> CreateAsync(int operationId, IEnumerable<LrpEndorsementDefinition> definitions)
		{
			if (!definitions.Any())
				return Array.Empty<LrpEndorsementModel>();

			var endorsements = new List<LrpEndorsementModel>();
			foreach (var definition in definitions)
			{
				endorsements.Add(await CreateModelAsync(operationId, definition));
			}

			using (var ts = Library.Helpers.Transaction.ReadCommitted())
			{
				var itemCount = await LrpEndorsementRepository.AddAsync(endorsements);
				if (itemCount != definitions.Count() || endorsements.Exists(x => x.Id == 0))
					throw new Exception("Failed to add endorsement to the repository.");

				await UpdateTags(operationId, endorsements);
				ts.Complete();
			}

			return endorsements;
		}

		public Task<bool> RecalculateAsync(LrpEndorsementModel endorsement)
			=> UpdateAsync(endorsement);

		public Task<bool> RecalculateAsync(int operationId, IEnumerable<LrpEndorsementModel> endorsements)
			=> UpdateAsync(endorsements);


		public async Task<LrpEndorsementModel> UpdateDefinitionAsync(LrpEndorsementModel existing, LrpEndorsementDefinition definition)
			=> (await UpdateDefinitionAsync(existing.OperationId, new[] { (existing, definition) })).FirstOrDefault();

		public async Task<IEnumerable<LrpEndorsementModel>> UpdateDefinitionAsync(int operationId, IEnumerable<(LrpEndorsementModel Endorsement, LrpEndorsementDefinition Definition)> endorsementDefinitions)
		{
			if (!endorsementDefinitions.Any())
				return Array.Empty<LrpEndorsementModel>();

			var endorsements = new List<LrpEndorsementModel>();
			foreach (var (existing, definition) in endorsementDefinitions)
			{
				var endorsement = await CreateModelAsync(
					existing.OperationId,
					definition
				);

				endorsement.AipImported = existing.AipImported;
				endorsement.Updated = DateTime.UtcNow;
				endorsement.Id = existing.Id;
				endorsements.Add(endorsement);
			}

			using (var ts = Library.Helpers.Transaction.ReadCommitted())
			{
				var updated = await LrpEndorsementRepository.UpdateAsync(endorsements);
				if (updated != endorsementDefinitions.Count())
					throw new Exception("Failed to updated endorsement.");

				await UpdateTags(operationId, endorsements);
				ts.Complete();
			}

			return endorsements;
		}

		#region Update

		public Task<bool> UpdateAsync(LrpEndorsementModel endorsement)
		{
			return UpdateAsync(new[] { endorsement });
		}

		public async Task<bool> UpdateAsync(IEnumerable<LrpEndorsementModel> endorsements)
		{
			using (var ts = Library.Helpers.Transaction.Suppress())
			{
				await Task.WhenAll(
					endorsements.Select(UpdateModelAsync)
				);
			}

			endorsements.ForEach(x => x.Updated = DateTime.Now);
			return await LrpEndorsementRepository.UpdateAsync(endorsements) == endorsements.Count();
		}

		#endregion

		private T CalculateQuote<T>(
			LrpCalculationParameters parameters,
			Value_Special_ADM_A00630_LrpRate dailyRate,
			IEnumerable<Report_USDA_RMA_Actuarial_Data_Master_A00070_Subsidy_Percent_Annual> subsidyPercent
		)
			where T : LrpQuote, new()
		{
			var quote = new T();

			var enUSCulture = System.Globalization.CultureInfo.GetCultureInfo("en-US");

			quote.SubsidyPercent = subsidyPercent.FirstOrDefault(x =>
				Convert.ToDecimal(x.RangeLowValue, enUSCulture) <= dailyRate.LivestockCoverageLevelPercent &&
				Convert.ToDecimal(x.RangeHighValue, enUSCulture) >= dailyRate.LivestockCoverageLevelPercent
			)?.SubsidyPercent ?? 0;

			quote.InsuredSharePercent = parameters.InsuredSharePercent;
			quote.NumberOfHead = parameters.NumberOfHead;
			quote.TargetWeight = parameters.TargetWeight;
			quote.CoveragePriceCwt = dailyRate.CoveragePrice;
			quote.CoveragePercent = dailyRate.LivestockCoverageLevelPercent;
			quote.PracticeCode = dailyRate.PracticeCode;
			quote.ExpectedPriceCwt = dailyRate.ExpectedEndingValueAmount ?? 0;
			quote.EndorsementLength = (int)dailyRate.EndorsementLengthCount;
			quote.EndDate = dailyRate.EndDate.GetValueOrDefault().Date;
			quote.PracticeCode = dailyRate.PracticeCode;
			quote.LivestockRate = dailyRate.LivestockRate;
			quote.SalesEffectiveDate = dailyRate.SalesEffectiveDate;

			quote.Liability = LrpCalculations.Liability(
				quote.NumberOfHead,
				quote.TargetWeight,
				quote.CoveragePriceCwt,
				parameters.InsuredSharePercent
			);

			quote.SubsidyAmount = LrpCalculations.Subsidy(
				quote.TotalPremium,
				quote.SubsidyPercent,
				parameters.ConservationCompliance,
				parameters.IsBeginner
			);

			quote.ConservationComplianceSubsidyReduction = LrpCalculations.ConservationComplianceSubsidyReduction(
				quote.TotalPremium,
				quote.SubsidyPercent,
				parameters.ConservationCompliance
			);

			return quote;
		}

		private async Task CalculateFuturesAsync(
			LrpQuote quote,
			LrpCalculationParameters parameters,
			IEnumerable<OptionsExpirationDates> futuresExpirationDates
		)
		{
			OptionsExpirationDates optionExpiration = null;
			if (parameters.PutMonthSelection == MonthSelection.FirstAfter)
				optionExpiration = futuresExpirationDates
					.OrderBy(x => x.LastTradeDate)
					.FirstOrDefault(x => x.LastTradeDate > quote.EndDate);
			else
				optionExpiration = futuresExpirationDates
					.OrderByDescending(x => x.LastTradeDate)
					.FirstOrDefault(x => x.LastTradeDate <= quote.EndDate);

			if (optionExpiration == null)
				return;

			var futurePrice = (
					await UdmService.GetValueFuturesDirect(
						parameters.Commodity.FuturesProduct(),
						optionExpiration.ContractMonth.Date,
						parameters.SalesEffectiveDate.Date
					)
				)
				.FirstOrDefault();

			if (futurePrice == null)
				return;

			quote.FuturesPriceCwt = futurePrice.ClosePrice;
			quote.CoverageMinusFuturesPriceCwt = LrpCalculations.CoverageMinusFutures(
				parameters.PutBasisSelection == BasisSelection.LrpCoveragePrice
					? quote.CoveragePriceCwt
					: quote.ExpectedPriceCwt,
				quote.FuturesPriceCwt.GetValueOrDefault()
			);
			quote.ComparisonMonth = optionExpiration.ContractMonth.Date;

			var priceOptions = await UdmService.GetValueOptionsDirect(
					OptionTypeId,
					parameters.Commodity.OptionsProduct(),
					optionExpiration.ContractMonth.Date,
					parameters.SalesEffectiveDate.Date
				);

			if (!priceOptions.Any())
				return;

			CalculateFutures(quote, parameters, futurePrice, priceOptions);
		}

		private void CalculateFutures(
			LrpQuote quote,
			LrpCalculationParameters parameters,
			ValueFutures futurePrice,
			IEnumerable<ValueOptions> valueOptions
		)
		{
			var compareValue = quote.CoveragePercent;
			if (parameters.PutStrikeSelection == StrikeSelection.StrikeLevel)
				compareValue = quote.CoveragePriceCwt;

			var priceOption =
					valueOptions.Select(option =>
					{
						var futureClosePrice = futurePrice.ClosePrice.GetValueOrDefault(option.Strike);

						var cmeCoveragePercent = 0M;
						if (futureClosePrice > 0)
							cmeCoveragePercent = option.Strike / futureClosePrice;

						return new
						{
							CmeCoveragePercent = cmeCoveragePercent,
							CmePutComparisonPrice = option.Strike,
							CmePutPremiumPrice = option.SettlePrice,
							SelectorValue = parameters.PutStrikeSelection == StrikeSelection.StrikeLevel ? option.Strike : cmeCoveragePercent,
							LrpPremium = option.SettlePrice.GetValueOrDefault() == 0 ? 0 : LrpCalculations.LrpPremium(quote.ProducerPremiumCwt, option.SettlePrice.GetValueOrDefault())
						};
					}
				)
				.OrderBy(x => x.SelectorValue)
				.FirstOrDefault(x => x.SelectorValue >= compareValue);

			if (priceOption == null)
				return;

			quote.CmePutComparisonStrikePrice = priceOption.CmePutComparisonPrice;
			quote.CmePutPremiumPrice = priceOption.CmePutPremiumPrice;
			quote.LrpPremium = priceOption.LrpPremium;
		}

		private async Task<IEnumerable<UDM_Report_USDA_RMA_Actuarial_Data_Master_A00620_Lrp_Actual_Ending_Value_Daily>> GetActualPrices(IEnumerable<int> reinsuranceYears, CommodityCode commodity, RiskPremium.TypeCode.LrpEnum commodityTypeCode, IEnumerable<DateTime> endDates)
		{
			var tasks = endDates
				.Distinct()
				.Batch(7)
				.Select(async endDatesBatch =>
					await UdmService.GetLrpActualPrices(
						reinsuranceYears,
						commodity,
						InterpolateTypeCode(commodity, commodityTypeCode),
						endDatesBatch
					)
				);

			return (await Task.WhenAll(tasks))
				.SelectMany(x => x)
				.ToList();
		}

		private async Task<IEnumerable<Value_Special_ADM_A00630_LrpRate>> GetRateForEndorsementLengthAsync(
			IEnumerable<DateTime> salesEffectiveDates,
			CommodityCode commodity,
			TypeCode.LrpEnum commodityTypeCode,
			int endorsementLength
		)
		{
			var tasks = salesEffectiveDates
				.Distinct()
				.Batch(7)
				.Select(
					batch => UdmService.GetAdmLrpRateAsync(
						salesEffectiveDates: batch,
						commodity: commodity,
						typeCode: InterpolateTypeCode(commodity, commodityTypeCode),
						endorsementLengthCount: endorsementLength
					)
				);

			return InterpolateSwineRates(
				(await Task.WhenAll(tasks)).SelectMany(x => x)
			);
		}

		public async Task<IEnumerable<Value_Special_ADM_A00630_LrpRate>> GetRateForEndorsementLengthAsync(
			DateTime salesEffectiveDate,
			CommodityCode commodity,
			TypeCode.LrpEnum typeCode,
			int endorsementLength
		)
		{
			var lrpRates = (await GetLrpRateAsync(salesEffectiveDate))
				.Where(x => int.Parse(x.CommodityCode) == (int)commodity && int.Parse(x.TypeCode) == (int)typeCode && x.EndorsementLengthCount == endorsementLength && x.LivestockCoverageLevelPercent.HasValue)
				.OrderByDescending(x => x.LivestockCoverageLevelPercent.Value)
				.ToList();

			return lrpRates;
		}

		private async Task<IEnumerable<Value_Special_ADM_A00630_LrpRate>> GetLrpRateForCoverageRankAsync(
			DateTime salesEffectiveDate,
			CommodityCode commodity,
			TypeCode.LrpEnum typeCode,
			int coveragePercentageRank
		)
		{
			return (await GetLrpRateAsync(salesEffectiveDate))
				.Where(x => int.Parse(x.CommodityCode) == (int)commodity && int.Parse(x.TypeCode) == (int)typeCode)
				.GroupBy(x => x.EndorsementLengthCount)
				.Select(
					x => x
						.OrderByDescending(x => x.LivestockCoverageLevelPercent)
						.ElementAtOrDefault(coveragePercentageRank - 1)
				)
				.Where(x => x != null)
				.OrderBy(x => x.EndorsementLengthCount)
				.ToList();
		}

		private async Task<IEnumerable<Value_Special_ADM_A00630_LrpRate>> GetLrpRateAsync(
			DateTime salesEffectiveDate,
			CommodityCode commodity,
			TypeCode.LrpEnum typeCode
		)
		{
			return (await GetLrpRateAsync(salesEffectiveDate))
				.Where(x => int.Parse(x.CommodityCode) == (int)commodity && int.Parse(x.TypeCode) == (int)typeCode)
				.OrderByDescending(x => x.LivestockCoverageLevelPercent).ThenBy(x => x.TypeCode).ThenBy(x => x.EndorsementLengthCount)
				.ToList();
		}

		public async Task<IEnumerable<Value_Special_ADM_A00630_LrpRate>> GetLrpRateAsync(DateTime salesEffectiveDate)
		{
			var rates = await UdmService.GetAdmLrpRateAsync(salesEffectiveDates: new[] { salesEffectiveDate });
			return InterpolateSwineRates(rates);
		}

		public DateTime GetSalesEffectiveDate(DateTime salesEffectiveDate)
		{
			while (salesEffectiveDate.DayOfWeek is DayOfWeek.Saturday or DayOfWeek.Sunday)
				salesEffectiveDate = salesEffectiveDate.AddDays(-1);

			return salesEffectiveDate;
		}

		private LrpHistoricEndorsementCoverageViewModel CalculateHistoricAvg(IEnumerable<LrpQuote> historicData)
		{
			if (historicData.Any())
			{
				return new LrpHistoricEndorsementCoverageViewModel
				{
					CoveragePriceCwt = Math.Round(historicData.Average(x => x.CoveragePriceCwt), 2),
					CoveragePercent = Math.Round(historicData.Average(x => x.CoveragePercent.GetValueOrDefault()), 4),
					ProducerPremium = Math.Round(historicData.Average(x => x.ProducerPremium), 2),
					ProducerPremiumCwt = Math.Round(historicData.Average(x => x.ProducerPremiumCwt), 2),
					ProducerPremiumHead = Math.Round(historicData.Average(x => x.ProducerPremiumHead), 2),
					NetGuaranteeCwt = Math.Round(historicData.Average(x => x.NetGuaranteeCwt), 2),
					NetGuaranteeHead = Math.Round(historicData.Average(x => x.NetGuaranteeHead), 2),
					ExpectedPrice = Math.Round(historicData.Average(x => x.ExpectedPrice), 2),
					InsuredValue = Math.Round(historicData.Average(x => x.Liability), 2),
					ActualPriceCwt = Math.Round(historicData.Average(x => x.ActualPriceCwt.GetValueOrDefault()), 2),
					GrossIndemnityCwt = Math.Round(historicData.Average(x => x.GrossIndemnityCwt.GetValueOrDefault()), 2),
					GrossIndemnityHead = Math.Round(historicData.Average(x => x.GrossIndemnityHead.GetValueOrDefault()), 2),
					GrossIndemnity = Math.Round(historicData.Average(x => x.GrossIndemnity.GetValueOrDefault()), 2),
					CoveragePriceHead = Math.Round(historicData.Average(x => x.CoveragePriceHead), 2),
					NetGuarantee = Math.Round(historicData.Average(x => x.NetGuarantee), 2),
					ExpectedPriceCwt = Math.Round(historicData.Average(x => x.ExpectedPriceCwt), 2),
					ExpectedPriceHead = Math.Round(historicData.Average(x => x.ExpectedPriceHead), 2),
					ActualPriceHead = Math.Round(historicData.Average(x => x.ActualPriceHead), 2),
					ActualPrice = Math.Round(historicData.Average(x => x.ActualPrice), 2)
				};
			}

			return new LrpHistoricEndorsementCoverageViewModel();
		}

		/// <summary>
		/// Gets the dates for the previous <param name="numberOfDays"/> from the <param name="salesEffectiveDate"/> for every year between salesEffectiveDates and 2008.
		/// </summary>
		/// <param name="salesEffectiveDate"></param>
		/// <returns></returns>
		private IEnumerable<DateTime> GetHistoricSalesEffectiveDates(DateTime salesEffectiveDate, int numberOfDays = 1)
		{
			var salesEffectiveDateList = new List<DateTime>();

			while (salesEffectiveDate.Year >= 2008)
			{
				salesEffectiveDateList.Add(salesEffectiveDate);

				for (int i = 1; i < numberOfDays; i++)
				{
					salesEffectiveDateList.Add(salesEffectiveDate.AddDays(-i));
				}

				salesEffectiveDate = salesEffectiveDate.AddYears(-1);
			}

			return salesEffectiveDateList;
		}

		/// <summary>
		/// Selects an actual available sales effective date in <paramref name="availableSalesEffectiveDates"/> for each year as low as 2008 for the provided <param name="salesEffectiveDate"></param>
		/// </summary>
		/// <param name="salesEffectiveDate"></param>
		/// <param name="availableSalesEffectiveDates"></param>
		/// <returns></returns>
		private IEnumerable<DateTime> GetActualSalesEffectiveDates(DateTime salesEffectiveDate, IEnumerable<DateTime> availableSalesEffectiveDates)
		{
			availableSalesEffectiveDates = availableSalesEffectiveDates.OrderByDescending(x => x.Date);

			return GetHistoricSalesEffectiveDates(salesEffectiveDate)
				.Select(
					targetSalesEffectiveDate => availableSalesEffectiveDates.FirstOrDefault(x => x.Date <= targetSalesEffectiveDate.Date && x.Date >= targetSalesEffectiveDate.Date.AddDays(-6))
				)
				.ToList();
		}

		private async Task SetActualPriceAsync<T>(
			CommodityCode commodity,
			TypeCode.LrpEnum typeCode,
			IEnumerable<T> items,
			Func<T, DateTime> salesEffectiveDateSelector,
			Func<T, DateTime> endDateSelector,
			Action<T, decimal?, DateTime?> actualPriceAndDateSetter
		)
		{
			var actualPrices = await GetActualPrices(
				items.Select(x => salesEffectiveDateSelector(x).GetReinsuranceYear()).Distinct(),
				commodity,
				typeCode,
				items.Select(x => endDateSelector(x)).Distinct()
			);

			var estimatedPrices = await ValueDailyRepository.ListLastCollectedValuesByReportPeriodAsync(
				filter =>
				{
					filter.AreaID = 1;
					filter.Active = true;
					filter.Product = commodity.EndingValueProduct();
					filter.ReportPeriods = items.Select(x => endDateSelector(x).Date).Distinct();
				}
			);

			foreach (var item in items)
			{
				decimal? actualPriceCwt = null;
				DateTime? actualPriceDate = null;
				var actualPrice = actualPrices.FirstOrDefault(
					x => x.ReinsuranceYear == salesEffectiveDateSelector(item).GetReinsuranceYear() && x.EndDate == endDateSelector(item)
				);

				if (actualPrice is not null && actualPrice.ActualEndingValueAmount.HasValue)
				{
					actualPriceCwt = actualPrice.ActualEndingValueAmount.Value;
					actualPriceDate = actualPrice.ReleasedDate;
				}
				else
				{
					var estimatedPrice = estimatedPrices.FirstOrDefault(x => x.ReportPeriod == endDateSelector(item).Date);
					if (estimatedPrice is not null)
					{
						actualPriceCwt = (decimal)estimatedPrice.Value;
						if (commodity == CommodityCode.FeederCattle)
							actualPriceCwt *= priceAdjustmentFactor[typeCode];
					}
				}

				actualPriceAndDateSetter(item, actualPriceCwt, actualPriceDate);
			}
		}

		public async Task<LrpAnalysisMonthlyViewModel> AnalysisMonthlyAsync(LrpAnalysisParameters parameters)
		{
			var lrpRates = await GetMonthlyAnalysisLrpRate(parameters);

			var average = GetAverage(lrpRates, parameters.NumberOfHead, parameters.TargetWeight);
			return new LrpAnalysisMonthlyViewModel() { Months = average };
		}

		public async Task<LrpAnalysisYearlyModel> AnalysisYearlyAsync(LrpAnalysisParameters parameters)
		{
			var lrpAnalysisYearlyModel = new LrpAnalysisYearlyModel();

			var valueMonthly = await ValueMonthlyRepository.ListAsync(
				filter =>
				{
					filter.Product = LrpCommodityMap.CommodityProductDictionary[(parameters.Commodity, parameters.CommodityType)];
					filter.ReportPeriodYearFrom = parameters.YearFrom;
					filter.ReportPeriodYearTo = parameters.YearTo;
				}
			);
			var lrpRatesTask = GetYearlyAnalysisLrpRate(parameters);

			var yearMonthModels = GetAverage(valueMonthly, x => (x.ReportPeriod.Year, x.ReportPeriod.Month), parameters.NumberOfHead, parameters.TargetWeight);
			var yearMonthAverages = GetAverage<(int year, int month)>(
				await lrpRatesTask,
				x => (x.EndDate.Value.Year, x.EndDate.Value.Month),
				parameters.NumberOfHead,
				parameters.TargetWeight
			);
			MapModels(
				yearMonthModels,
				yearMonthAverages,
				x => (x.Key.year, x.Key.month),
				(monthAverage, model) =>
				{
					model.Month = monthAverage.Key.month;
					model.NumberOfHead = parameters.NumberOfHead;
					model.TargetWeight = parameters.TargetWeight;
					model.ExpectedPriceCwt = monthAverage.Value.ExpectedPriceCwt;
					model.ProducerPremiumCwt = monthAverage.Value.ProducerPremiumCwt;
					model.NetGuaranteeCwt = monthAverage.Value.NetGuaranteeCwt;
					model.CoveragePriceCwt = monthAverage.Value.CoveragePriceCwt;
				}
			);

			lrpAnalysisYearlyModel.Years = yearMonthModels
				.GroupBy(x => x.Key.Year)
				.Select(
					x => new LrpAnalysisYearModel()
					{
						Year = x.Key,
						Months = x.Select(x => x.Value)
					}
				)
				.OrderBy(x => x.Year);

			var monthModels = GetAverage(valueMonthly, x => x.ReportPeriod.Month, parameters.NumberOfHead, parameters.TargetWeight);
			var monthAverages = GetAverage(await lrpRatesTask, parameters.NumberOfHead, parameters.TargetWeight);
			MapModels(
				monthModels,
				monthAverages,
				x => x.Month,
				(monthAverage, model) =>
				{
					model.Month = monthAverage.Month;
					model.NumberOfHead = parameters.NumberOfHead;
					model.TargetWeight = parameters.TargetWeight;
					model.ExpectedPriceCwt = monthAverage.ExpectedPriceCwt;
					model.ProducerPremiumCwt = monthAverage.ProducerPremiumCwt;
					model.NetGuaranteeCwt = monthAverage.NetGuaranteeCwt;
					model.CoveragePriceCwt = monthAverage.CoveragePriceCwt;
				}
			);

			lrpAnalysisYearlyModel.Average = monthModels.Select(x => x.Value);

			if (monthModels.Any())
			{
				lrpAnalysisYearlyModel.YearFrom = lrpAnalysisYearlyModel.Average.Min(x => x.YearFrom);
				lrpAnalysisYearlyModel.YearTo = lrpAnalysisYearlyModel.Average.Max(x => x.YearTo);
			}

			return lrpAnalysisYearlyModel;
		}

		private IEnumerable<LrpAnalysisMonthAverageModel> GetAverage(IEnumerable<Value_Special_ADM_A00630_LrpRate> rates, int numberOfHead, decimal targetWeight)
			=> GetAverage(rates, x => x.EndDate.Value.Month, numberOfHead, targetWeight).Select(x => x.Value);

		private Dictionary<TKey, LrpAnalysisMonthAverageModel> GetAverage<TKey>(IEnumerable<Value_Special_ADM_A00630_LrpRate> rates, Func<Value_Special_ADM_A00630_LrpRate, TKey> keyFunc, int numberOfHead, decimal targetWeight)
		{
			return rates
				.GroupBy(keyFunc)
				.ToDictionary(
					g => g.Key,
					rateGroup => new LrpAnalysisMonthAverageModel()
					{
						NumberOfHead = numberOfHead,
						TargetWeight = targetWeight,
						Month = rateGroup.First().EndDate.Value.Month,
						EndDate = rateGroup.First().EndDate.Value,
						YearFrom = rateGroup.Min(x => x.EndDate.Value.Year),
						YearTo = rateGroup.Max(x => x.EndDate.Value.Year),
						ActualPriceCwt = rateGroup.Select(x => x.ActualEndingValueAmount).Where(x => x != 0).DefaultIfEmpty(null).Average(x => x).Round(2),
						ExpectedPriceCwt = rateGroup.Select(x => x.ExpectedEndingValueAmount).Where(x => x != 0).DefaultIfEmpty(null).Average(x => x).Round(2),
						ProducerPremiumCwt = rateGroup.Select(x => x.ProducerCostPerCwtAmount).Where(x => x != 0).DefaultIfEmpty(null).Average(x => x).Round(2),
						NetGuaranteeCwt = rateGroup.Select(x => x.NetGuarantee).Where(x => x != 0).DefaultIfEmpty(null).Average(x => x).Round(2),
						CoveragePriceCwt = rateGroup.Select(x => x.CoveragePrice).Where(x => x != 0).DefaultIfEmpty(0).Average(x => x).Round(2)
					}
				);
		}

		private Dictionary<TKey, LrpAnalysisMonthAverageModel> GetAverage<TKey>(IEnumerable<ValueMonthly> valueMonthly, Func<ValueMonthly, TKey> keyFunc, int numberOfHead, decimal targetWeight)
		{
			return valueMonthly
				.GroupBy(keyFunc)
				.ToDictionary(
					g => g.Key,
					rateGroup => new LrpAnalysisMonthAverageModel()
					{
						NumberOfHead = numberOfHead,
						TargetWeight = targetWeight,
						Month = rateGroup.First().ReportPeriod.Month,
						EndDate = rateGroup.First().ReportPeriod,
						YearFrom = rateGroup.Min(x => x.ReportPeriod.Year),
						YearTo = rateGroup.Max(x => x.ReportPeriod.Year),
						ActualPriceCwt = rateGroup.Select(x => (decimal?)x.Value).Where(x => x != 0).DefaultIfEmpty(null).Average(x => x).Round(2),
					}
				);
		}

		private void MapModels<T, TKey, TModel>(
			Dictionary<TKey, TModel> models, IEnumerable<T> values,
			Func<T, TKey> keyFunc,
			Action<T, TModel> mapAction
		)
			where TModel : class, new()
		{
			foreach (var value in values)
			{
				var key = keyFunc(value);
				if (!models.TryGetValue(key, out TModel model))
				{
					model = new TModel();
					models.Add(key, model);
				}

				mapAction(value, model);
			}
		}

		public async Task<IEnumerable<Value_Special_ADM_A00630_LrpRate>> GetMonthlyAnalysisLrpRate(LrpAnalysisParameters parameters)
		{
			var lrpRates = await UdmService.GetAdmLrpRateAsync(
				salesEffectiveDates: new[] { parameters.Date },
				commodity: parameters.Commodity,
				typeCode: InterpolateTypeCode(parameters.Commodity, parameters.CommodityType),
				deductibleCategory: parameters.Deductible * 2,
				productId: 113014
			);

			return InterpolateAnalysisRates(lrpRates, parameters.Commodity, parameters.CommodityType);
		}

		public async Task<IEnumerable<Value_Special_ADM_A00630_LrpRate>> GetYearlyAnalysisLrpRate(LrpAnalysisParameters parameters)
		{
			var lrpRates = await UdmService.GetAdmLrpRateAsync(
				commodity: parameters.Commodity,
				typeCode: InterpolateTypeCode(parameters.Commodity, parameters.CommodityType),
				deductibleCategory: parameters.Deductible * 2,
				productId: 113026,
				salesEffectiveDateMonth: parameters.Date.Month
			);

			if (parameters.YearTo.HasValue)
				lrpRates = lrpRates.Where(x => x.EndDate.Value.Year <= parameters.YearTo);

			if (parameters.YearFrom.HasValue)
				lrpRates = lrpRates.Where(x => x.EndDate.Value.Year >= parameters.YearFrom);

			return InterpolateAnalysisRates(lrpRates.ToList(), parameters.Commodity, parameters.CommodityType);
		}

		private IEnumerable<Value_Special_ADM_A00630_LrpRate> InterpolateAnalysisRates(
			IEnumerable<Value_Special_ADM_A00630_LrpRate> rates,
			CommodityCode? commodity,
			TypeCode.LrpEnum? commodityType
		)
		{
			if (commodity != CommodityCode.Swine)
				return rates;

			if (commodityType != TypeCode.LrpEnum.AllTypes)
				return rates;

			return InterpolateSwineRates(rates)
				.Where(x =>
					{
						var commodity = Library.Helpers.EnumExtensions.ToEnum<CommodityCode>(int.Parse(x.CommodityCode));
						var typeCode = Library.Helpers.EnumExtensions.ToEnum<TypeCode.LrpEnum>(int.Parse(x.TypeCode));

						return commodity != CommodityCode.Swine || typeCode == TypeCode.LrpEnum.AllTypes;
					}
				)
				.ToList();
		}


		private IEnumerable<Value_Special_ADM_A00630_LrpRate> InterpolateSwineRates(
			IEnumerable<Value_Special_ADM_A00630_LrpRate> rates
		)
		{
			var swineRates = rates
				.Where(x => int.Parse(x.CommodityCode) == (int)CommodityCode.Swine)
				.GroupBy(x => x.SalesEffectiveDate);

			return rates
				.Union(
					swineRates.SelectMany(x =>
						x
						// .Where(y => y.EndorsementLengthCount != 30 || (int.Parse(y.TypeCode) == (int)TypeCode.LrpEnum.UnbornSwine && y.EndorsementLengthCount == 30))
						.Where(
							y =>
							{
								var typeCode = Library.Helpers.EnumExtensions.ToEnum<TypeCode.LrpEnum>(int.Parse(y.TypeCode));

								return y.EndorsementLengthCount switch
								{
									< 30 => typeCode == TypeCode.LrpEnum.NoTypeSpecified,
									>= 30 => typeCode == TypeCode.LrpEnum.UnbornSwine
								};
							}
						)
						.Select(y => { var z = y.ShallowClone(); z.TypeCode = Library.Helpers.EnumExtensions.GetValue(TypeCode.LrpEnum.AllTypes); return z; })
					)
				)
				.ToList();
		}

		private TypeCode.LrpEnum? InterpolateTypeCode(CommodityCode commodity, TypeCode.LrpEnum commodityType)
		{
			return commodity == CommodityCode.Swine && commodityType == TypeCode.LrpEnum.AllTypes ? null : commodityType;
		}

		public async Task<LrpMeasureStatistics> AnalysisHistoricAsync(
			LrpAnalysisParameters parameters,
			LrpMeasureType measure
		)
		{
			var lrpRates = await UdmService.GetAdmLrpRateAsync(
				endDateYear: parameters.Date.Year,
				endDateMonth: parameters.Date.Month,
				commodity: parameters.Commodity,
				typeCode: InterpolateTypeCode(parameters.Commodity, parameters.CommodityType),
				deductibleCategory: parameters.Deductible * 2,
				productId: 113014
			);

			var statistics = new LrpMeasureStatistics(
				InterpolateAnalysisRates(lrpRates, parameters.Commodity, parameters.CommodityType),
				measure,
				parameters.NumberOfHead,
				parameters.TargetWeight
			);

			var averageLrpRates = await UdmService.GetAdmLrpRateAsync(
				endDateMonth: parameters.Date.Month,
				endDateYearFrom: parameters.YearFrom,
				endDateYearTo: parameters.YearTo,
				commodity: parameters.Commodity,
				typeCode: InterpolateTypeCode(parameters.Commodity, parameters.CommodityType),
				deductibleCategory: parameters.Deductible * 2,
				productId: 113026
			);

			var interpolatedAvarageSwineRates = InterpolateAnalysisRates(averageLrpRates, parameters.Commodity, parameters.CommodityType);

			var yearEndDates = interpolatedAvarageSwineRates
				.OrderByDescending(x => x.EndDate)
				.GroupBy(x => x.EndDate.Value.Year)
				.Select(x => x.First().EndDate.Value)
				.ToList();

			var yearEndDateRates = interpolatedAvarageSwineRates
				.Join(
					yearEndDates,
					rate => rate.EndDate.Value,
					endDate => endDate,
					(rate, endDate) => rate
				)
				.ToList();

			statistics.HistoricOverview = new HistorySummaryModel(yearEndDateRates, measure, parameters.NumberOfHead, parameters.TargetWeight);

			return statistics;
		}

		private Task<int> UpdateTags(int operationId, IEnumerable<LrpEndorsementModel> endorsements)
		{
			return TagService.UpdateTags(
				operationId,
				TagTable.LrpEndorsement,
				endorsements
					.Select(e => (e.Id, e.Tags?.Select(t => t.Name)))
					.ToList()
			);
		}
		public async Task<LatestPrices> GetMonthlyRates(LrpAnalasysFilter parameters)
		{
			var endDatesBatches = Enumerable.Range(0, 30)
				.Select(i => parameters.EndDate.AddDays(-i))
				.Batch(3);

			var salesEffectiveDatesBatches = Enumerable.Range(0, 30)
				.Select(i => parameters.Date.AddDays(-i))
				.Batch(3);

			var tasks = salesEffectiveDatesBatches
				.SelectMany(salesEffectiveBatch => endDatesBatches, (salesEffectiveBatch, endDateBatch) =>
					UdmService.GetAdmLrpRateAsync(
						salesEffectiveDates: salesEffectiveBatch,
						commodity: parameters.Commodity,
						typeCode: InterpolateTypeCode(parameters.Commodity, parameters.CommodityType),
						endDates: endDateBatch
					))
				.ToList();

			var lrpRates = await Task.WhenAll(tasks);

			var sortedRates = lrpRates.SelectMany(rates => rates)
				.Where(rate => rate != null && rate.EndDate > parameters.StartEndDate.Date.AddDays(-1) && rate.EndDate < parameters.EndDate.Date.AddDays(1))
				.OrderByDescending(rate => rate.SalesEffectiveDate)
				.ThenBy(rate => rate.EndDate)
				.ToList();

			if (parameters.CoveragePercentageRank.HasValue)
				sortedRates = sortedRates
					.Where(x => x != null && x.EndDate <= parameters.EndDate && x.LivestockCoverageLevelPercent.HasValue)
					.GroupBy(x => x.EndDate)
					.Select(
						x => x
						.OrderByDescending(x => x.LivestockCoverageLevelPercent)
						.ElementAtOrDefault((int)parameters.CoveragePercentageRank - 1)
					)
					.Where(x => x != null)
					.ToList();

			if (sortedRates.Any())
			{
				var latestRate = sortedRates[0];
				var previousRate = sortedRates.FirstOrDefault(rate => rate.EndDate != latestRate.EndDate);
				var last7DaysRates = sortedRates
						.GroupBy(rate => rate.EndDate)
						.OrderByDescending(g => g.Key)
						.Take(7)
						.SelectMany(g => g);

				var latestPrices = new LatestPrices
				{
					Latest = MapToLrpAnalysisMonthModel(latestRate, parameters),
					Previous = MapToLrpAnalysisMonthModel(previousRate, parameters),
					Last7Days = CalculateAverage(last7DaysRates, parameters),
					Last30Days = CalculateAverage(sortedRates, parameters)
				};

				return latestPrices;
			}

			return new LatestPrices();
		}
		private static LatestPriceAnalysisModel CalculateAverage(IEnumerable<Value_Special_ADM_A00630_LrpRate> data, LrpAnalasysModel parameters)
		{
			return new LatestPriceAnalysisModel
			{
				ActualPriceCwt = data.Average(item => item.ActualEndingValueAmount ?? 0),
				ExpectedPriceCwt = data.Average(item => item.ExpectedEndingValueAmount ?? 0),
				ProducerPremiumCwt = data.Average(item => item.ProducerCostPerCwtAmount ?? 0),
				NetGuaranteeCwt = data.Average(item => item.NetGuarantee ?? 0),
				CoveragePriceCwt = data.Average(item => item.CoveragePrice),
				NumberOfHead = parameters.NumberOfHead,
				TargetWeight = parameters.TargetWeight
			};
		}

		private static LatestPriceAnalysisModel MapToLrpAnalysisMonthModel(Value_Special_ADM_A00630_LrpRate data, LrpAnalasysModel parameters)
		{
			if (data == null)
				return new LatestPriceAnalysisModel();

			return new LatestPriceAnalysisModel
			{
				ActualPriceCwt = data.ActualEndingValueAmount,
				ExpectedPriceCwt = data.ExpectedEndingValueAmount,
				ProducerPremiumCwt = data.ProducerCostPerCwtAmount,
				NetGuaranteeCwt = data.NetGuarantee,
				CoveragePriceCwt = data.CoveragePrice,
				NumberOfHead = parameters.NumberOfHead,
				TargetWeight = parameters.TargetWeight,
				EndDate = (DateTime)data.EndDate,
				salesEffectiveDate = data.SalesEffectiveDate
			};
		}

		public async Task<ValueFuturesViewModel> HistoricalCMESpotPrices(DateTime salesEffectiveDate, CommodityCode commodity)
		{
			var selectedDatePrices = await UdmService.GetValueFuturesDirect(commodity.FuturesProduct(), default, salesEffectiveDate);

			if (selectedDatePrices == null || !selectedDatePrices.Any())
				return null;

			var previousDatePrices = await UdmService.GetValueFuturesDirect(commodity.FuturesProduct(), selectedDatePrices.Select(x => x.ContractMonth), new List<DateTime>() { default });

			var sortedList = previousDatePrices.OrderBy(x => x.ContractMonth)
												.ThenByDescending(x => x.ReportPeriod);

			List<ValueFutures> previousMatchPrices = new();
			foreach (var item in selectedDatePrices)
			{
				var match = sortedList.FirstOrDefault(x => x.ReportPeriod < item.ReportPeriod
														&& x.ContractMonth == item.ContractMonth
														&& x.ClosePrice.HasValue);

				if (match != null)
					previousMatchPrices.Add(match);
			}

			return new ValueFuturesViewModel(selectedDatePrices, previousMatchPrices);
		}
	}
}